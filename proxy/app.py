#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页。

功能要点：
- 按客户端 IP:端口建立会话，空闲超时（默认 5 秒）后过期；
- 建会话时把该会话固定到当前目标接收器（A/B），会话期间不随切换改变；
- 每个会话使用一条独立的、已 connect 的上游 UDP 套接字（独立本地端口），
  因此回包天然只能属于一个会话：只把该会话对应接收器的回包送回该客户端；
- 上游端口来自端口池，建连时在池内搜索可绑定端口：
  * 同一轮每个端口最多尝试绑定一次；
  * 遇到绑定冲突（如同机进程占用）继续找下一个，池内确实无可用端口才报用尽，
    地址解析失败等其它原因单独上报；
  * 并行建连各自预留不同端口，不得共用；
  * 因其他建连预留而跳过的端口，若在本轮结束前释放且不在冷却期，也会再检查；
  * 搜索期间已有会话照常收发和过期，释放的会话端口照常冷却；
  * 外部进程释放端口后，新会话无需重启即可使用（失败不进入任何黑名单）；
- 会话过期即关闭其上游端口，且端口进入冷却期不再分配，
  所以旧会话迟到的回包不可能落到同 IP:端口重建的新会话里；
- 报文与回包原样透传，不做任何修改；
- 当前目标保存在 SQLite，重启后保留；会话全在内存，重启即清空；
- 切换目标时先落库再生效，保存失败则保留原目标；
- 网页可让独立演示进程（与代理同一网络命名空间）真实占用/释放所选池内端口，
  再发真实 UDP 试传：接收器按试传标识回报实收报文的 UDP 源端口，
  代理对照本次绑定端口，本次回执到达且两端一致才标为通过。
"""

import asyncio
import atexit
import errno
import json
import os
import re
import secrets
import select
import socket
import sqlite3
import subprocess
import sys
import threading
import time

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ---------------- 配置 ----------------
UDP_LISTEN_HOST = os.environ.get("UDP_LISTEN_HOST", "0.0.0.0")
UDP_LISTEN_PORT = int(os.environ.get("UDP_LISTEN_PORT", "9000"))
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.db")
)
SESSION_IDLE_TIMEOUT = float(os.environ.get("SESSION_IDLE_TIMEOUT", "5"))
UPSTREAM_PORT_MIN = int(os.environ.get("UPSTREAM_PORT_MIN", "20000"))
UPSTREAM_PORT_MAX = int(os.environ.get("UPSTREAM_PORT_MAX", "29999"))
# 上游端口关闭后的冷却时间：冷却期内不再分配该端口，
# 保证旧会话迟到的回包不会被复用同端口的新会话收到。应大于接收器最大回包延迟。
UPSTREAM_PORT_COOLDOWN = float(os.environ.get("UPSTREAM_PORT_COOLDOWN", "30"))
# 一轮建连搜索端口池的最长耗时（主要用于等待并行建连释放预留端口）。
OPEN_SEARCH_TIMEOUT = float(os.environ.get("OPEN_SEARCH_TIMEOUT", "15"))
LOG_KEEP = int(os.environ.get("LOG_KEEP", "1000"))
TRIAL_KEEP = int(os.environ.get("TRIAL_KEEP", "100"))  # 页面保留的试传记录条数
OCCUPIER_PATH = os.environ.get(
    "OCCUPIER_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "port_occupier.py"),
)

RECEIVERS = {
    "A": (
        os.environ.get("RECEIVER_A_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_A_PORT", "9001")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_B_PORT", "9002")),
    ),
}
DEFAULT_TARGET = os.environ.get("DEFAULT_TARGET", "A")

# 试传报文标识：UDPTRIAL1:<16位hex>:<载荷>；回执：UDPTRIAL1:<id>:SEEN:<源端口>
TRIAL_MSG_RE = re.compile(rb"^UDPTRIAL1:([0-9a-f]{16}):")
TRIAL_RECEIPT_RE = re.compile(rb"^UDPTRIAL1:([0-9a-f]{16}):SEEN:(\d{1,5})$")


def fmt_addr(addr):
    return f"{addr[0]}:{addr[1]}"


def preview(data: bytes) -> str:
    """报文内容展示：可打印文本原样显示，否则用 hex。"""
    try:
        text = data.decode("utf-8")
        if all(ch.isprintable() or ch in "\r\n\t" for ch in text):
            return text if len(text) <= 200 else text[:200] + "…"
    except UnicodeDecodeError:
        pass
    hexed = data.hex()
    return "hex:" + (hexed if len(hexed) <= 200 else hexed[:200] + "…")


def fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


class OpenFailed(Exception):
    """建连失败（原因可直接展示）：地址解析失败、绑定失败等。"""


class PoolExhausted(OpenFailed):
    """端口池内确实无可用端口。"""


# ---------------- SQLite：目标持久化 + 报文日志 ----------------
class Store:
    """线程安全的 SQLite 存取。目标(target)落库，重启后保留。"""

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS message_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   client TEXT NOT NULL DEFAULT '',
                   session_id INTEGER NOT NULL DEFAULT 0,
                   direction TEXT NOT NULL,
                   receiver TEXT NOT NULL DEFAULT '',
                   payload TEXT NOT NULL DEFAULT '',
                   note TEXT NOT NULL DEFAULT '')"""
        )
        self._conn.commit()
        row = self._conn.execute("SELECT value FROM settings WHERE key='target'").fetchone()
        if row and row[0] in RECEIVERS:
            self._target = row[0]
        else:
            self._target = DEFAULT_TARGET if DEFAULT_TARGET in RECEIVERS else sorted(RECEIVERS)[0]
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (self._target,)
            )
            self._conn.commit()
        self._inserts = 0

    def get_target(self):
        with self._lock:
            return self._target

    def set_target(self, target):
        """先持久化，成功后才切换内存中的当前目标；失败则抛异常并保留原目标。"""
        if target not in RECEIVERS:
            raise ValueError(f"未知接收器: {target!r}")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (target,)
            )
            self._conn.commit()
            self._target = target

    def log(self, direction, client="", session_id=0, receiver="", payload="", note=""):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO message_log(ts,client,session_id,direction,receiver,payload,note)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (time.time(), client, session_id, direction, receiver, payload, note),
                )
                self._conn.commit()
                self._inserts += 1
                if self._inserts % 200 == 0:
                    self._conn.execute(
                        "DELETE FROM message_log WHERE id < "
                        "(SELECT COALESCE(MIN(id),0) FROM "
                        "(SELECT id FROM message_log ORDER BY id DESC LIMIT ?))",
                        (LOG_KEEP,),
                    )
                    self._conn.commit()
        except sqlite3.Error:
            pass  # 日志失败绝不影响转发

    def recent_messages(self, limit=100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,ts,client,session_id,direction,receiver,payload,note "
                "FROM message_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        cols = ("id", "ts", "client", "session_id", "direction", "receiver", "payload", "note")
        return [dict(zip(cols, r)) for r in rows]


# ---------------- 上游端口分配（预留/占用/冷却分离） ----------------
class PortAllocator:
    """显式管理上游本地端口。

    三种状态：reserved（建连预留中）、in_use（会话绑定使用中）、
    closed（关闭后冷却中）。关闭后冷却期内不再分配，
    确保旧会话迟到的回包不会被复用同端口的新会话收到。
    绑定失败（如被外部进程占用）只释放预留，不进入冷却、不进黑名单，
    外部进程释放端口后新会话无需重启即可使用。"""

    def __init__(self, low, high, cooldown):
        self.low, self.high, self.cooldown = low, high, cooldown
        self._next = low
        self._reserved = set()  # 建连预留（并行建连不得共用端口）
        self._in_use = set()    # 已绑定给会话
        self._closed = {}       # port -> 关闭时刻(monotonic)，冷却中
        self._lock = threading.Lock()

    def _available_locked(self, port, now):
        return (
            port not in self._in_use
            and port not in self._reserved
            and not (
                port in self._closed and now - self._closed[port] < self.cooldown
            )
        )

    def reserve_next(self, tried, skipped_reserved):
        """为这一轮搜索预留下一个候选端口；没有候选返回 None。
        tried：本轮已尝试过绑定的端口（同一轮每个端口最多尝试一次）；
        skipped_reserved：收集因被其他建连预留而跳过的端口。"""
        with self._lock:
            now = time.monotonic()
            for _ in range(self.high - self.low + 1):
                port = self._next
                self._next = self.low if self._next >= self.high else self._next + 1
                if port in tried or port in self._in_use:
                    continue
                if port in self._reserved:
                    skipped_reserved.add(port)
                    continue
                if not self._available_locked(port, now):
                    continue
                self._reserved.add(port)
                self._closed.pop(port, None)  # 冷却期已满，清除记录
                return port
        return None

    def confirm(self, port):
        """绑定成功：预留转为正式占用。"""
        with self._lock:
            self._reserved.discard(port)
            self._in_use.add(port)

    def discard(self, port):
        """绑定失败：只释放预留，不进入冷却（我们从未真正占用该端口）。"""
        with self._lock:
            self._reserved.discard(port)

    def release(self, port):
        """会话关闭：端口进入冷却期，冷却内不再分配。"""
        with self._lock:
            self._in_use.discard(port)
            self._reserved.discard(port)
            self._closed[port] = time.monotonic()

    def has_released_candidate(self, skipped_reserved, tried):
        """因预留被跳过的端口中，是否有已释放、不在冷却期且本轮未试过的。"""
        with self._lock:
            now = time.monotonic()
            return any(
                p not in tried and self._available_locked(p, now)
                for p in skipped_reserved
            )

    def any_reserved(self, skipped_reserved):
        """被跳过的端口中是否仍有被其他建连预留的（可稍等其释放）。"""
        with self._lock:
            return any(p in self._reserved for p in skipped_reserved)

    def stats(self):
        with self._lock:
            now = time.monotonic()
            cooling = sum(
                1 for t in self._closed.values() if now - t < self.cooldown
            )
            return {
                "min": self.low,
                "max": self.high,
                "total": self.high - self.low + 1,
                "in_use": len(self._in_use),
                "reserved": len(self._reserved),
                "cooling": cooling,
            }

    def exhaustion_report(self, conflicts):
        s = self.stats()
        return (
            f"上游 UDP 端口池用尽：池 {s['min']}-{s['max']} 共 {s['total']} 个端口，"
            f"会话占用 {s['in_use']} 个，冷却中 {s['cooling']} 个，"
            f"其他建连预留 {s['reserved']} 个，本轮绑定冲突 {conflicts} 个，"
            f"确实无可用端口"
        )


# ---------------- 试传核对 ----------------
class Trial:
    __slots__ = (
        "id", "client", "payload", "wait", "sock",
        "created", "created_ts", "finished", "finished_at",
        "state", "bound_port", "seen_port", "error",
    )

    def __init__(self, tid, client, payload, wait, sock):
        self.id = tid
        self.client = client
        self.payload = payload
        self.wait = wait
        self.sock = sock
        self.created = time.monotonic()
        self.created_ts = time.time()
        self.finished = False
        self.finished_at = None
        self.state = "pending"  # pending -> passed/mismatch/unconfirmed/failed
        self.bound_port = None  # 代理本次绑定的上游端口
        self.seen_port = None   # 接收器回报的实收源端口
        self.error = ""


class TrialRegistry:
    """试传记录：按试传标识核对代理绑定端口与接收器实收端口。
    并行试传各用各的回执；已结束试传的回执不再参与核对。"""

    def __init__(self, keep=TRIAL_KEEP):
        self._keep = keep
        self._trials = {}       # tid -> Trial
        self._by_client = {}    # client -> {tid}
        self._lock = threading.Lock()

    def create(self, tid, client, payload, wait, sock):
        t = Trial(tid, client, payload, wait, sock)
        with self._lock:
            self._trials[tid] = t
            self._by_client.setdefault(client, set()).add(tid)
            self._prune_locked()
        return t

    def _prune_locked(self):
        if len(self._trials) <= self._keep:
            return
        done = sorted(
            (t for t in self._trials.values() if t.finished),
            key=lambda t: t.created,
        )
        for t in done[: len(self._trials) - self._keep]:
            self._drop_locked(t)

    def _drop_locked(self, t):
        self._trials.pop(t.id, None)
        ids = self._by_client.get(t.client)
        if ids is not None:
            ids.discard(t.id)
            if not ids:
                self._by_client.pop(t.client, None)
        self._close_sock(t)

    @staticmethod
    def _close_sock(t):
        if t.sock is not None:
            try:
                t.sock.close()
            except OSError:
                pass
            t.sock = None

    def note_bound(self, tid, port):
        """试传报文实际发出时记录本次绑定的上游端口。"""
        with self._lock:
            t = self._trials.get(tid)
            if t is None or t.finished:
                return
            t.bound_port = port

    def note_receipt(self, tid, seen_port):
        """接收器回执到达：记录实收端口并与本次绑定端口对照。返回日志备注。"""
        with self._lock:
            t = self._trials.get(tid)
            if t is None:
                return f"未知试传 {tid} 的回执，不参与核对"
            if t.finished:
                return f"试传 {tid} 已结束，其回执不再参与核对"
            t.seen_port = seen_port
            if t.bound_port is None:
                return f"试传 {tid} 回执先到，绑定端口尚未记录"
            t.finished = True
            t.finished_at = time.monotonic()
            if seen_port == t.bound_port:
                t.state = "passed"
                return f"试传 {tid} 核对通过：两端端口均为 {seen_port}"
            t.state = "mismatch"
            return f"试传 {tid} 端口不符：代理绑定 {t.bound_port}，接收器实收 {seen_port}"

    def note_open_failed(self, client, reason):
        """建连失败：该客户端名下未结束的试传标记失败并记录原因。"""
        with self._lock:
            now = time.monotonic()
            for tid in list(self._by_client.get(client, ())):
                t = self._trials.get(tid)
                if t is not None and not t.finished:
                    t.state = "failed"
                    t.error = reason
                    t.finished = True
                    t.finished_at = now

    def sweep(self):
        """等不到本次回执的试传标记未确认；回收已结束试传的客户端套接字。"""
        now = time.monotonic()
        with self._lock:
            for t in self._trials.values():
                if not t.finished and now - t.created > t.wait:
                    t.state = "unconfirmed"
                    t.finished = True
                    t.finished_at = now
                if (
                    t.finished
                    and t.sock is not None
                    and t.finished_at is not None
                    and now - t.finished_at > SESSION_IDLE_TIMEOUT + 2
                ):
                    # 会话应已过期后再关闭客户端套接字，避免端口被新试传复用而串会话
                    self._close_sock(t)

    def snapshot(self):
        now = time.monotonic()
        with self._lock:
            items = sorted(self._trials.values(), key=lambda t: t.created, reverse=True)
            return [
                {
                    "id": t.id,
                    "time": fmt_ts(t.created_ts),
                    "client": t.client,
                    "payload": t.payload,
                    "state": t.state,
                    "bound_port": t.bound_port,
                    "seen_port": t.seen_port,
                    "error": t.error,
                    "elapsed": round((t.finished_at or now) - t.created, 2),
                }
                for t in items
            ]


# ---------------- 独立演示进程：真实占用/释放池内端口 ----------------
class OccupierManager:
    """按需启动的独立演示进程（与代理同处一个网络命名空间，
    在 docker 中即同一容器内的子进程），真实 bind 所选池内 UDP 端口。
    通过 stdin/stdout 的 JSON 行协议控制；代理退出时其 stdin 关闭，自动退出。"""

    def __init__(self, path):
        self._path = path
        self._proc = None
        self._lock = threading.Lock()

    def _ensure_locked(self):
        if self._proc is not None and self._proc.poll() is None:
            return self._proc
        self._proc = subprocess.Popen(
            [sys.executable, "-I", self._path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # stderr 继承，崩溃可见于代理日志
        )
        return self._proc

    def command(self, obj, timeout=3.0):
        with self._lock:
            try:
                proc = self._ensure_locked()
                proc.stdin.write(json.dumps(obj).encode() + b"\n")
                proc.stdin.flush()
                ready, _, _ = select.select([proc.stdout], [], [], timeout)
                if not ready:
                    raise RuntimeError("演示进程无响应")
                line = proc.stdout.readline()
                if not line:
                    raise RuntimeError("演示进程已退出")
                return json.loads(line)
            except Exception:
                proc, self._proc = self._proc, None
                if proc is not None:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                raise

    def held_ports(self):
        """当前占用列表；进程未启动时不启动它，直接返回空。"""
        if self._proc is None or self._proc.poll() is not None:
            return []
        try:
            return self.command({"cmd": "list"}).get("held", [])
        except Exception:
            return []

    def shutdown(self):
        with self._lock:
            proc, self._proc = self._proc, None
        if proc is not None and proc.poll() is None:
            try:
                proc.stdin.close()  # EOF：演示进程自行退出并释放全部端口
            except Exception:
                pass
            try:
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass


# ---------------- 会话与转发 ----------------
class Session:
    __slots__ = (
        "id", "client_addr", "receiver_name", "receiver_addr",
        "upstream_port", "transport", "state", "closed",
        "created", "last_activity", "pending",
    )

    def __init__(self, sid, client_addr):
        self.id = sid
        self.client_addr = client_addr
        self.receiver_name = ""
        self.receiver_addr = None
        self.upstream_port = None
        self.transport = None
        self.state = "opening"  # opening -> open -> closed
        self.closed = False
        self.created = time.monotonic()
        self.last_activity = self.created
        self.pending = []


class ClientProtocol(asyncio.DatagramProtocol):
    """面向遥测终端的 UDP 端。"""

    def __init__(self, state):
        self.state = state

    def datagram_received(self, data, addr):
        self.state.on_client_datagram(data, addr)

    def error_received(self, exc):
        pass


class UpstreamProtocol(asyncio.DatagramProtocol):
    """每个会话一条、已 connect 到其接收器的上游 UDP 端。
    只有该会话对应接收器的回包会到达这里。"""

    def __init__(self, state):
        self.state = state
        self.session = None

    def datagram_received(self, data, addr):
        sess = self.session
        if sess is not None:
            self.state.on_upstream_datagram(sess, data)

    def error_received(self, exc):
        pass  # 如对端 ICMP 不可达，忽略


class ProxyState:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.client_transport = None
        self.sessions = {}  # client_addr -> Session（仅事件循环线程修改）
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)

    async def start(self):
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ClientProtocol(self),
            local_addr=(UDP_LISTEN_HOST, UDP_LISTEN_PORT),
        )
        self.client_transport = transport
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {UDP_LISTEN_HOST}:{UDP_LISTEN_PORT}", flush=True)
        print(
            f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，当前目标 {self.store.get_target()}，"
            f"上游端口池 {UPSTREAM_PORT_MIN}-{UPSTREAM_PORT_MAX}"
            f"（冷却 {UPSTREAM_PORT_COOLDOWN}s，搜索超时 {OPEN_SEARCH_TIMEOUT}s）",
            flush=True,
        )

    # ---- 客户端方向 ----
    def on_client_datagram(self, data, addr):
        now = time.monotonic()
        with self.lock:
            sess = self.sessions.get(addr)
            if sess is not None and sess.closed:
                sess = None
            if (sess is not None and sess.state == "open"
                    and now - sess.last_activity > SESSION_IDLE_TIMEOUT):
                self._close_session(sess, "idle-timeout")
                sess = None
            if sess is None:
                # 建会话：固定到“当前”目标接收器
                sess = Session(self.next_session_id, addr)
                self.next_session_id += 1
                self.sessions[addr] = sess
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._forward(sess, data)

    async def _open_session(self, sess):
        target = self.store.get_target()  # 建会话时固定接收器
        host, port = RECEIVERS[target]
        client = fmt_addr(sess.client_addr)
        try:
            transport, proto, upstream_port = await self._connect_upstream(
                target, host, port, sess
            )
        except OpenFailed as exc:
            reason = str(exc)
        except Exception as exc:  # 兜底：任何意外都按建连失败上报
            reason = f"建连失败：{exc}"
        else:
            reason = None
        if reason is not None:
            self.store.log("event", client=client, session_id=sess.id,
                           note=f"会话建立失败：{reason}")
            trials.note_open_failed(client, reason)
            self.close_session(sess, f"open-failed: {reason}")
            return
        with self.lock:
            if sess.closed:
                transport.close()
                self.ports.release(upstream_port)
                return
            sess.receiver_name = target
            sess.receiver_addr = (host, port)
            sess.upstream_port = upstream_port
            sess.transport = transport
            sess.state = "open"
            proto.session = sess
            pending, sess.pending = sess.pending, []
        self.store.log("event", client=client, session_id=sess.id, receiver=target,
                       note=f"会话建立，固定到接收器 {target}（上游端口 {upstream_port}）")
        for data in pending:
            self._forward(sess, data)

    async def _connect_upstream(self, target, host, port, sess):
        """解析接收器地址，再在端口池内搜索可绑定端口。
        解析失败与端口池用尽分别上报。"""
        try:
            infos = await self.loop.getaddrinfo(
                host, port,
                family=socket.AF_INET, type=socket.SOCK_DGRAM,
                proto=socket.IPPROTO_UDP,
            )
        except socket.gaierror as exc:
            raise OpenFailed(
                f"接收器 {target} 地址 {host}:{port} 解析失败：{exc}"
            ) from exc
        remote = infos[0][4]
        return await self._open_upstream(remote, sess)

    async def _open_upstream(self, remote, sess):
        """在端口池内搜索可绑定端口。返回 (transport, protocol, port)。

        规则：同一轮每个端口最多尝试绑定一次；绑定冲突（EADDRINUSE）
        继续找下一个；因其他建连预留而跳过的端口，若在本轮结束前释放
        且不在冷却期，也会再检查；池内确实无可用端口才报 PoolExhausted。
        搜索期间不断让出事件循环，已有会话照常收发和过期。"""
        tried = set()            # 本轮已尝试过绑定的端口
        skipped_reserved = set()  # 因被其他建连预留而跳过的端口
        conflicts = 0
        attempts = 0
        deadline = time.monotonic() + OPEN_SEARCH_TIMEOUT
        while True:
            if sess.closed:
                raise OpenFailed("建连期间会话已关闭")
            port = self.ports.reserve_next(tried, skipped_reserved)
            if port is None:
                # 一遍扫完仍无候选：因预留跳过的端口若已释放且不在冷却期，再检查
                if self.ports.has_released_candidate(skipped_reserved, tried):
                    continue
                # 仍有端口被其他建连预留：稍等其在本轮结束前释放
                if (self.ports.any_reserved(skipped_reserved)
                        and time.monotonic() < deadline):
                    await asyncio.sleep(0.05)
                    continue
                raise PoolExhausted(self.ports.exhaustion_report(conflicts))
            tried.add(port)
            attempts += 1
            if attempts % 64 == 0:
                await asyncio.sleep(0)  # 长搜索时让出事件循环
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.bind(("0.0.0.0", port))
            except OSError as exc:
                sock.close()
                self.ports.discard(port)
                if exc.errno == errno.EADDRINUSE:
                    conflicts += 1
                    continue  # 绑定冲突：继续找下一个端口
                raise OpenFailed(f"绑定上游端口 {port} 失败：{exc}") from exc
            try:
                sock.connect(remote)  # 已 connect：只收该接收器的回包
                transport, proto = await self.loop.create_datagram_endpoint(
                    lambda: UpstreamProtocol(self), sock=sock
                )
            except Exception as exc:
                sock.close()
                self.ports.discard(port)
                raise OpenFailed(f"创建上游连接（端口 {port}）失败：{exc}") from exc
            self.ports.confirm(port)
            return transport, proto, port

    def _forward(self, sess, data):
        with self.lock:
            if sess.closed or sess.transport is None:
                return
            sess.last_activity = time.monotonic()
            upstream_port = sess.upstream_port
        m = TRIAL_MSG_RE.match(data)
        if m and upstream_port is not None:
            # 试传报文实际发出：记录本次绑定的上游端口
            trials.note_bound(m.group(1).decode(), upstream_port)
        try:
            sess.transport.sendto(data)  # 已 connect，原样发往该会话的接收器
        except Exception as exc:
            self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           note=f"转发失败: {exc}")
            return
        self.store.log("c2r", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))

    # ---- 接收器方向 ----
    def on_upstream_datagram(self, sess, data):
        now = time.monotonic()
        with self.lock:
            if sess.closed or self.sessions.get(sess.client_addr) is not sess:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="旧会话迟到的回包，已丢弃")
                return
            if now - sess.last_activity > SESSION_IDLE_TIMEOUT:
                self._close_session(sess, "idle-timeout")
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="回包到达时会话已过期，已丢弃")
                return
            sess.last_activity = now
        note = ""
        m = TRIAL_RECEIPT_RE.match(data)
        if m:
            # 试传回执：对照本次绑定端口（回执本身仍原样转发）
            note = trials.note_receipt(m.group(1).decode(), int(m.group(2)))
        self.client_transport.sendto(data, sess.client_addr)  # 原样送回该客户端
        self.store.log("r2c", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data), note=note)

    # ---- 会话生命周期 ----
    def _close_session(self, sess, reason):
        """调用方需持有 self.lock。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.client_addr) is sess:
            del self.sessions[sess.client_addr]
        transport, sess.transport = sess.transport, None
        port, sess.upstream_port = sess.upstream_port, None
        if transport is not None:
            transport.close()  # 迟到的回包到此端口将被系统丢弃
        if port is not None:
            self.ports.release(port)  # 进入冷却期，冷却内不再分配
        self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, note=f"会话关闭（{reason}）")

    def close_session(self, sess, reason):
        with self.lock:
            self._close_session(sess, reason)

    async def _sweeper(self):
        while True:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            with self.lock:
                expired = [s for s in self.sessions.values()
                           if not s.closed and now - s.last_activity > SESSION_IDLE_TIMEOUT]
                for sess in expired:
                    self._close_session(sess, "idle-timeout")
            trials.sweep()

    def snapshot_sessions(self):
        now = time.monotonic()
        with self.lock:
            return [
                {
                    "id": s.id,
                    "client": fmt_addr(s.client_addr),
                    "receiver": s.receiver_name or "?",
                    "upstream_port": s.upstream_port,
                    "state": s.state,
                    "idle": round(now - s.last_activity, 1),
                    "age": round(now - s.created, 1),
                }
                for s in sorted(self.sessions.values(), key=lambda x: x.id)
            ]


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
state = ProxyState(store)
trials = TrialRegistry()
occupier = OccupierManager(OCCUPIER_PATH)
app = Flask(__name__)
app.secret_key = os.urandom(16)
atexit.register(occupier.shutdown)

DIRECTION_LABELS = {
    "c2r": "客户端→接收器",
    "r2c": "接收器→客户端",
    "event": "事件",
    "drop": "丢弃",
}


def _format_message(m):
    m = dict(m)
    m["ts"] = fmt_ts(m["ts"])
    m["direction"] = DIRECTION_LABELS.get(m["direction"], m["direction"])
    return m


@app.get("/")
def index():
    return render_template(
        "index.html",
        target=store.get_target(),
        receivers={k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        idle_timeout=SESSION_IDLE_TIMEOUT,
        udp_listen_port=UDP_LISTEN_PORT,
        pool_min=UPSTREAM_PORT_MIN,
        pool_max=UPSTREAM_PORT_MAX,
    )


@app.get("/api/state")
def api_state():
    return jsonify({
        "target": store.get_target(),
        "receivers": {k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        "idle_timeout": SESSION_IDLE_TIMEOUT,
        "pool": state.ports.stats(),
        "held_ports": occupier.held_ports(),
        "sessions": state.snapshot_sessions(),
        "messages": [_format_message(m) for m in store.recent_messages(100)],
        "trials": trials.snapshot(),
    })


@app.post("/switch")
def switch():
    target = request.form.get("target", "")
    try:
        store.set_target(target)
        store.log("event", note=f"网页切换目标为接收器 {target}")
        flash(f"已切换到接收器 {target}。新会话走 {target}，旧会话继续走原接收器直至过期。", "ok")
    except Exception as exc:
        flash(f"切换保存失败，保留原目标 {store.get_target()}。原因：{exc}", "error")
    return redirect(url_for("index"))


def parse_port_spec(spec):
    """解析 "20000-20007,20010" 形式的端口选择，仅限池内端口。"""
    ports = set()
    try:
        for part in (spec or "").replace("，", ",").split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                a_s, _, b_s = part.partition("-")
                a, b = int(a_s), int(b_s)
                if a > b:
                    a, b = b, a
                ports.update(range(a, b + 1))
            else:
                ports.add(int(part))
    except ValueError:
        raise ValueError(f"无法解析端口选择：{spec!r}（示例：{UPSTREAM_PORT_MIN}-{UPSTREAM_PORT_MIN + 7}）")
    if len(ports) > 500:
        raise ValueError("一次最多选择 500 个端口")
    for p in ports:
        if not (UPSTREAM_PORT_MIN <= p <= UPSTREAM_PORT_MAX):
            raise ValueError(
                f"端口 {p} 不在端口池 {UPSTREAM_PORT_MIN}-{UPSTREAM_PORT_MAX} 内，仅支持池内端口"
            )
    return sorted(ports)


@app.post("/api/occupy")
def api_occupy():
    """让独立演示进程在代理的网络命名空间真实占用所选池内端口。"""
    try:
        ports = parse_port_spec(request.form.get("ports", ""))
        if not ports:
            raise ValueError("未选择端口")
        resp = occupier.command({"cmd": "hold", "ports": ports})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})
    return jsonify({
        "ok": True,
        "results": resp.get("results", {}),
        "held": resp.get("held", []),
    })


@app.post("/api/release")
def api_release():
    """释放演示进程占用的所选（或全部）池内端口。"""
    try:
        if request.form.get("all"):
            resp = occupier.command({"cmd": "release_all"})
        else:
            ports = parse_port_spec(request.form.get("ports", ""))
            if not ports:
                raise ValueError("未选择端口")
            resp = occupier.command({"cmd": "release", "ports": ports})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})
    return jsonify({
        "ok": True,
        "results": resp.get("results", {}),
        "held": resp.get("held", []),
    })


def start_trial(payload, wait):
    """从网页后端经代理发一条真实 UDP 试传报文（带试传标识）。"""
    tid = secrets.token_hex(8)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    host = UDP_LISTEN_HOST if UDP_LISTEN_HOST not in ("", "0.0.0.0") else "127.0.0.1"
    sock.connect((host, UDP_LISTEN_PORT))
    client = fmt_addr(sock.getsockname())
    trials.create(tid, client, payload, wait, sock)
    wire = b"UDPTRIAL1:" + tid.encode() + b":" + payload.encode()
    sock.send(wire)
    return tid


@app.post("/api/trial")
def api_trial():
    """发真实 UDP 试传；并行多条时各用各的回执。"""
    try:
        wait = min(max(float(request.form.get("wait", "") or 5), 0.5), 60.0)
    except ValueError:
        wait = 5.0
    try:
        count = min(max(int(request.form.get("count", "") or 1), 1), 50)
    except ValueError:
        count = 1
    payload = (request.form.get("payload") or "PING")[:120]
    ids = [start_trial(payload, wait) for _ in range(count)]
    return jsonify({"ok": True, "ids": ids})


# ---------------- 启动：asyncio 线程 + Flask 主线程 ----------------
def _run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state.loop = loop
    loop.run_until_complete(state.start())
    loop.run_forever()


def main():
    t = threading.Thread(target=_run_loop, name="udp-proxy-loop", daemon=True)
    t.start()
    print(f"[proxy] 操作网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
