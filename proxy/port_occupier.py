#!/usr/bin/env python3
"""端口占用演示进程（独立进程，与代理同处一个网络命名空间）。

由代理按需启动：在 docker 中是代理容器内的子进程，与代理共享网络命名空间，
因此它 bind 的 UDP 端口对代理真实可见（代理绑定同一端口会得到 EADDRINUSE）。

控制协议：stdin 每行一个 JSON 命令，stdout 每行一个 JSON 应答。
  {"cmd": "hold", "ports": [20000, ...]}   占用端口（逐个报告成败）
  {"cmd": "release", "ports": [...]}       释放指定端口
  {"cmd": "release_all"}                   释放全部端口
  {"cmd": "list"}                          查询当前占用
  {"cmd": "quit"}                          退出
stdin 关闭（EOF）时自动退出并释放全部端口。

注意：套接字不设置 SO_REUSEADDR，确保占用是“硬”占用。
"""

import json
import socket
import sys

held = {}  # port -> socket


def hold(ports):
    results = {}
    for p in ports:
        p = int(p)
        if p in held:
            results[str(p)] = "ok（此前已占用）"
            continue
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.bind(("0.0.0.0", p))
        except OSError as exc:
            s.close()
            results[str(p)] = f"失败：{exc.strerror or exc}"
            continue
        held[p] = s
        results[str(p)] = "ok"
    return results


def release(ports):
    results = {}
    for p in ports:
        p = int(p)
        s = held.pop(p, None)
        if s is None:
            results[str(p)] = "未占用"
            continue
        s.close()
        results[str(p)] = "ok"
    return results


def release_all():
    for s in held.values():
        try:
            s.close()
        except OSError:
            pass
    held.clear()


def handle(req):
    cmd = req.get("cmd")
    if cmd == "hold":
        return {"ok": True, "results": hold(req.get("ports", []))}
    if cmd == "release":
        return {"ok": True, "results": release(req.get("ports", []))}
    if cmd == "release_all":
        release_all()
        return {"ok": True, "results": {}}
    if cmd == "list":
        return {"ok": True}
    if cmd == "quit":
        return None
    return {"ok": False, "error": f"未知命令 {cmd!r}"}


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            resp = handle(json.loads(line))
        except Exception as exc:
            resp = {"ok": False, "error": str(exc)}
        if resp is None:  # quit
            break
        resp["held"] = sorted(held)
        try:
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()
        except BrokenPipeError:
            break
    release_all()


if __name__ == "__main__":
    main()
