#!/usr/bin/env python3
"""本地端到端验证：不依赖 Docker，直接以子进程启动两个接收器和代理，
逐项验证需求行为。用法：python3 scripts/verify_local.py
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROXY_PY = os.path.join(ROOT, "proxy", "app.py")
RECEIVER_PY = os.path.join(ROOT, "receiver", "receiver.py")

UDP_PORT = 19000
HTTP_PORT = 18080
BASE = f"http://127.0.0.1:{HTTP_PORT}"

POOL_MIN, POOL_MAX = 21000, 21011  # 12 个端口的小池，便于占满
IDLE = 2.0       # 会话空闲超时（秒）
COOLDOWN = 3.0   # 上游端口冷却（秒）

_failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def http_get(path):
    with urllib.request.urlopen(BASE + path, timeout=10) as r:
        return json.load(r)


def http_post(path, data):
    with urllib.request.urlopen(
        BASE + path, data=urllib.parse.urlencode(data).encode(), timeout=40
    ) as r:
        return r.status


def http_post_json(path, data):
    req = urllib.request.Request(
        BASE + path, data=urllib.parse.urlencode(data).encode()
    )
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.load(r)


def wait_http_up(timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            http_get("/api/state")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def udp_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def send_recv(sock, payload: bytes, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def log_entries(client=None, direction=None, text=None):
    msgs = http_get("/api/state")["messages"]
    out = []
    for m in msgs:
        if client and m["client"] != client:
            continue
        if direction and m["direction"] != direction:
            continue
        if text and text not in m["payload"] and text not in m["note"]:
            continue
        out.append(m)
    return out


def occupy(spec):
    return http_post_json("/api/occupy", {"ports": spec})


def release(spec=None, release_all=False):
    data = {"all": "1"} if release_all else {"ports": spec}
    return http_post_json("/api/release", data)


def held_ports():
    return http_get("/api/state")["held_ports"]


def pool_stats():
    return http_get("/api/state")["pool"]


def start_trial(payload="PING", wait=5, count=1):
    r = http_post_json("/api/trial", {"payload": payload, "wait": wait, "count": count})
    return r["ids"]


def get_trial(tid):
    for t in http_get("/api/state")["trials"]:
        if t["id"] == tid:
            return t
    return None


def wait_trial(tid, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        t = get_trial(tid)
        if t is not None and t["state"] != "pending":
            return t
        time.sleep(0.2)
    return None


def session_port(client):
    for s in http_get("/api/state")["sessions"]:
        if s["client"] == client:
            return s["upstream_port"]
    return None


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag, receiver_b_host="127.0.0.1"):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_B_HOST=receiver_b_host, RECEIVER_B_PORT="19002",
        SESSION_IDLE_TIMEOUT=str(IDLE), DB_PATH=db_path,
        UPSTREAM_PORT_MIN=str(POOL_MIN), UPSTREAM_PORT_MAX=str(POOL_MAX),
        UPSTREAM_PORT_COOLDOWN=str(COOLDOWN),
        OPEN_SEARCH_TIMEOUT="5",
    )
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def stop(proc, timeout=10):
    try:
        proc.terminate()
        proc.wait(timeout=timeout)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def main():
    tmp = tempfile.mkdtemp(prefix="proxy-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    procs = []
    receiver_b = None
    proxy = None
    try:
        receiver_a = start_receiver("A", 19001, tmp)
        receiver_b = start_receiver("B", 19002, tmp)
        procs += [receiver_a, receiver_b]
        proxy = start_proxy(db_path, tmp, "run1")
        procs.append(proxy)
        if not wait_http_up():
            check("代理启动", False, f"日志见 {tmp}")
            return 1
        print(f"--- 环境就绪（日志目录 {tmp}）---")

        # 1. 初始目标为 A，报文原样往返（含二进制）
        st = http_get("/api/state")
        check("初始目标为 A", st["target"] == "A")
        c1 = udp_client()
        client1 = f"127.0.0.1:{c1.getsockname()[1]}"
        m1 = b"m1-hello"
        check("报文原样回传", send_recv(c1, m1) == m1)
        blob = bytes(range(256))
        check("二进制报文原样透传", send_recv(c1, blob) == blob)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m1-hello")
        check("m1 实际去向为接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 2. 同机进程占住连续 8 个池内端口：新会话仍应绑定后面的空位
        print("--- 等待 c1 会话过期后占用连续 8 个池内端口 ---")
        time.sleep(IDLE + 0.5)
        r = occupy(f"{POOL_MIN}-{POOL_MIN + 7}")
        check("演示进程占用连续 8 个池内端口",
              r.get("ok") and sorted(r.get("held", [])) == list(range(POOL_MIN, POOL_MIN + 8)),
              str(r))
        c2 = udp_client()
        client2 = f"127.0.0.1:{c2.getsockname()[1]}"
        check("连续 8 端口被占时新会话报文仍有回包", send_recv(c2, b"m2-skip") == b"m2-skip")
        time.sleep(0.2)
        p2 = session_port(client2)
        check("代理跳过被占端口绑定后面的空位",
              p2 is not None and POOL_MIN + 8 <= p2 <= POOL_MAX, f"bound={p2}")

        # 3. 试传：两端端口一致判通过
        tid = start_trial()[0]
        t = wait_trial(tid)
        check("试传通过且两端端口一致",
              t and t["state"] == "passed"
              and t["bound_port"] is not None
              and t["bound_port"] == t["seen_port"], str(t))
        check("试传绑定端口避开被占端口",
              t and t["bound_port"] not in range(POOL_MIN, POOL_MIN + 8), str(t))

        # 4. 池内确实无可用端口才报用尽；搜索期间已有会话照常收发
        release(release_all=True)
        print("--- 等待会话过期且冷却结束 ---")
        time.sleep(IDLE + COOLDOWN + 0.5)
        c3 = udp_client()
        client3 = f"127.0.0.1:{c3.getsockname()[1]}"
        check("c3 建会话", send_recv(c3, b"m3-a") == b"m3-a")
        time.sleep(0.2)
        p3 = session_port(client3)
        check("c3 拿到上游端口", p3 is not None, f"sessions={http_get('/api/state')['sessions']}")
        others = [p for p in range(POOL_MIN, POOL_MAX + 1) if p != p3]
        r = occupy(",".join(map(str, others)))
        check("演示进程占满其余 11 个端口",
              r.get("ok") and len(r.get("held", [])) == 11, str(r))
        check("已有会话在搜索前照常收发", send_recv(c3, b"m3-b") == b"m3-b")
        tid2 = start_trial(wait=4)[0]
        t2 = wait_trial(tid2)
        check("池占满时建连失败并报端口池用尽",
              t2 and t2["state"] == "failed" and "端口池用尽" in t2["error"], str(t2))
        check("用尽搜索期间已有会话照常收发", send_recv(c3, b"m3-c") == b"m3-c")

        # 5. 外部进程释放端口后，新会话无需重启即可使用
        px = held_ports()[0]
        r = release(str(px))
        check("释放一个端口", r.get("ok") and px not in r.get("held", []), str(r))
        tid3 = start_trial()[0]
        t3 = wait_trial(tid3)
        check("释放后新会话立即可用（试传通过，绑定即所释放端口）",
              t3 and t3["state"] == "passed" and t3["bound_port"] == px
              and t3["seen_port"] == px, str(t3))
        release(release_all=True)

        # 6. 并行试传：各用各的回执，端口互不相同
        tids = start_trial(count=5)
        results = [wait_trial(t) for t in tids]
        check("并行试传全部通过", all(x and x["state"] == "passed" for x in results),
              str(results))
        ports = {x["bound_port"] for x in results if x}
        check("并行建连不共用端口", len(ports) == 5, str(ports))
        check("并行试传各自两端一致",
              all(x["bound_port"] == x["seen_port"] for x in results), str(results))

        # 7. 切换目标：旧会话走原接收器，新会话走新接收器
        c5 = udp_client()
        client5 = f"127.0.0.1:{c5.getsockname()[1]}"
        check("c5 首包走 A", send_recv(c5, b"m5-stick") == b"m5-stick")
        http_post("/switch", {"target": "B"})
        check("切换后当前目标为 B", http_get("/api/state")["target"] == "B")
        check("旧会话报文仍有回包", send_recv(c5, b"m5-stick2") == b"m5-stick2")
        time.sleep(0.2)
        e = log_entries(client5, "客户端→接收器", "m5-stick2")
        check("切换后旧会话继续走接收器 A", e and e[0]["receiver"] == "A", str(e))
        c4 = udp_client()
        client4 = f"127.0.0.1:{c4.getsockname()[1]}"
        check("新会话报文有回包", send_recv(c4, b"m4-new") == b"m4-new")
        time.sleep(0.2)
        e = log_entries(client4, "客户端→接收器", "m4-new")
        check("新会话走接收器 B", e and e[0]["receiver"] == "B", str(e))

        # 8. 旧会话迟到的回包：同 IP:端口 重建会话也收不到
        c6 = udp_client()
        client6 = f"127.0.0.1:{c6.getsockname()[1]}"
        t0 = time.time()
        c6.send(b"DELAY:4:late1")          # 接收器 4 秒后才回包；会话 2 秒即过期
        time.sleep(IDLE + 0.5 - (time.time() - t0))
        check("过期后新会话报文有回包", send_recv(c6, b"ping2", timeout=3) == b"ping2")
        got_late = False
        c6.settimeout(0.2)
        while time.time() - t0 < 5.5:
            try:
                if b"late1" in c6.recv(65535):
                    got_late = True
            except socket.timeout:
                pass
        check("旧会话迟到的回包未送达重建的会话", not got_late)
        time.sleep(0.3)
        e = log_entries(client6, "接收器→客户端", "late1")
        check("日志中无 late1 的回包下发记录", not e, str(e))

        # 9. 等不到本次回执：未确认
        stop(receiver_b)  # 接收器 B 宕机：会话能建，回执等不到
        procs.remove(receiver_b)
        tid4 = start_trial(wait=2)[0]
        t4 = wait_trial(tid4, timeout=10)
        check("等不到本次回执显示未确认",
              t4 and t4["state"] == "unconfirmed", str(t4))

        # 10. 重启：目标保留、会话清空
        receiver_b = start_receiver("B", 19002, tmp)
        procs.append(receiver_b)
        time.sleep(0.7)  # 等接收器 B 完成启动监听
        c7 = udp_client()
        c7.send(b"before-restart")
        c7.settimeout(3)
        c7.recv(65535)
        stop(proxy)
        procs.remove(proxy)
        proxy = start_proxy(db_path, tmp, "run2")
        procs.append(proxy)
        check("代理重启后恢复", wait_http_up())
        st = http_get("/api/state")
        check("重启后目标保留为 B", st["target"] == "B")
        check("重启后会话已清空", st["sessions"] == [])
        check("重启后报文仍有回包", send_recv(c7, b"after-restart", timeout=3) == b"after-restart")

        # 11. 地址解析失败单独上报（区别于端口池用尽）
        stop(proxy)
        procs.remove(proxy)
        proxy = start_proxy(db_path, tmp, "run3",
                            receiver_b_host="no-such-host.invalid")
        procs.append(proxy)
        check("代理（坏 B 地址）重启后恢复", wait_http_up())
        tid5 = start_trial(wait=10)[0]  # 当前目标仍是 B，地址无法解析
        t5 = wait_trial(tid5, timeout=20)
        check("地址解析失败单独上报",
              t5 and t5["state"] == "failed" and "解析失败" in t5["error"], str(t5))
        http_post("/switch", {"target": "A"})
        tid6 = start_trial()[0]
        t6 = wait_trial(tid6)
        check("切回 A 后试传通过", t6 and t6["state"] == "passed", str(t6))

        # 11b. 首页渲染
        html = urllib.request.urlopen(BASE, timeout=10).read().decode("utf-8")
        check("首页包含端口池演示与试传区域",
              "端口池演示" in html and "发送试传" in html and "占用" in html)

        # 12. 进程内单元行为：存储失败保留目标 / 试传核对 / 端口分配器
        sys.path.insert(0, os.path.join(ROOT, "proxy"))
        os.environ["DB_PATH"] = os.path.join(tmp, "unit.db")
        import app as proxy_app

        s = proxy_app.Store(os.path.join(tmp, "t.db"))
        s.set_target("B")
        s._conn.close()  # 模拟存储故障
        failed = False
        try:
            s.set_target("A")
        except Exception:
            failed = True
        check("保存失败时切换报错且保留原目标", failed and s.get_target() == "B")

        reg = proxy_app.TrialRegistry(keep=10)
        reg.create("a" * 16, "127.0.0.1:5001", "PING", 5, None)
        reg.note_bound("a" * 16, 21000)
        note = reg.note_receipt("a" * 16, 21000)
        ta = reg._trials["a" * 16]
        check("核对：两端一致判通过", ta.state == "passed" and "通过" in note, note)
        reg.create("b" * 16, "127.0.0.1:5002", "PING", 5, None)
        reg.note_bound("b" * 16, 21001)
        reg.note_receipt("b" * 16, 21002)
        tb = reg._trials["b" * 16]
        check("核对：两端不一致报不符", tb.state == "mismatch")
        note = reg.note_receipt("b" * 16, 21001)  # 已结束试传的回执
        check("已结束试传的回执不再参与核对",
              tb.state == "mismatch" and "不再参与核对" in note, note)
        reg.create("c" * 16, "127.0.0.1:5003", "PING", 5, None)
        reg.note_open_failed("127.0.0.1:5003", "上游 UDP 端口池用尽")
        tc = reg._trials["c" * 16]
        check("建连失败记录原因", tc.state == "failed" and "用尽" in tc.error)
        reg.create("d" * 16, "127.0.0.1:5004", "PING", 0.05, None)
        time.sleep(0.1)
        reg.sweep()
        check("超时未收到回执判未确认", reg._trials["d" * 16].state == "unconfirmed")
        note = reg.note_receipt("e" * 16, 21000)
        check("未知试传回执不参与核对", "不参与核对" in note, note)

        pa = proxy_app.PortAllocator(1, 4, 10.0)
        tried, skipped = set(), set()
        p1 = pa.reserve_next(tried, skipped)
        tried.add(p1)
        p2 = pa.reserve_next(tried, skipped)
        tried.add(p2)
        check("分配器同轮给出不同端口", p1 != p2 and not skipped)
        pa.confirm(p1)   # p1 正式占用
        pa.discard(p2)   # p2 绑定失败：不冷却、可再分
        tried2, skipped2 = set(), set()
        q = [pa.reserve_next(tried2, skipped2) for _ in range(3)]
        none = pa.reserve_next(tried2, skipped2)
        check("池满时无候选且记录被预留端口",
              none is None and all(x is not None for x in q)
              and skipped2 == set(q),
              f"q={q} skipped={skipped2}")
        pa.discard(q[2])  # 其他建连释放了预留端口
        check("预留释放后可再检查",
              pa.has_released_candidate(skipped2, tried={x for x in q if x != q[2]}))
        check("仍有预留时如实报告", pa.any_reserved(skipped2))
        pa.discard(q[0])
        pa.discard(q[1])
        check("预留清空", not pa.any_reserved(skipped2))
        pa.release(p1)   # 会话关闭：进入冷却
        check("冷却期内不可再分配", not pa.has_released_candidate({p1}, tried=set()))
        st = pa.stats()
        check("池统计正确", st["in_use"] == 0 and st["cooling"] == 1, str(st))

        # 13. 真实搜索：被其他建连预留而跳过的端口，本轮结束前释放且不在冷却期→被检查
        import asyncio

        class _FakeSess:
            closed = False

        async def search_with_late_release():
            st2 = proxy_app.ProxyState(proxy_app.Store(os.path.join(tmp, "u2.db")))
            st2.ports = proxy_app.PortAllocator(23000, 23001, 60.0)
            st2.loop = asyncio.get_running_loop()
            ext = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            ext.bind(("0.0.0.0", 23001))  # 外部进程占用 23001
            p = st2.ports.reserve_next(set(), set())  # 23000 被另一建连预留
            assert p == 23000

            async def release_soon():
                await asyncio.sleep(0.3)
                st2.ports.discard(23000)  # 对方绑定失败释放（不冷却）

            asyncio.get_running_loop().create_task(release_soon())
            transport, _, port = await st2._open_upstream(("127.0.0.1", 19001),
                                                          _FakeSess())
            transport.close()
            st2.ports.release(port)
            ext.close()
            return port

        port = asyncio.run(search_with_late_release())
        check("被预留端口在本轮结束前释放后被检查并绑定", port == 23000, f"port={port}")

        # 14. 真实搜索：预留始终不释放→报用尽（而非误用或死等）
        proxy_app.OPEN_SEARCH_TIMEOUT = 0.6

        async def search_reserved_forever():
            st3 = proxy_app.ProxyState(proxy_app.Store(os.path.join(tmp, "u3.db")))
            st3.ports = proxy_app.PortAllocator(23002, 23002, 60.0)
            st3.loop = asyncio.get_running_loop()
            st3.ports.reserve_next(set(), set())  # 唯一端口被另一建连预留
            try:
                await st3._open_upstream(("127.0.0.1", 19001), _FakeSess())
            except proxy_app.PoolExhausted as exc:
                return str(exc)
            return None

        msg = asyncio.run(search_reserved_forever())
        check("预留不释放时如实报用尽", msg is not None and "端口池用尽" in msg, str(msg))

    finally:
        for p in procs:
            stop(p)

    print("---")
    if _failures:
        print(f"共 {len(_failures)} 项失败: {_failures}")
        return 1
    print("全部验证通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
