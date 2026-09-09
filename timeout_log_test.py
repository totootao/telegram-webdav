"""验证「超时日志」是否说清楚了三件事：哪个访问、有没有重试、重试了几次。

背景：标准库 http.server 在 rfile 读/写超时时，是在 *它自己* 的 handle_one_request
内部 except 掉 TimeoutError 并直接 log_error("Request timed out: %r") 的，
外层覆写的 handle_one_request 根本感知不到，日志里只剩一行没有主体、没有路径的重罪：
    172.17.0.1 - - [09/Sep/2026 02:16:58] Request timed out: TimeoutError('timed out')
本测试逐条验证改造后的输出：
  A. 请求处理中超时   → 详细打印 客户端/请求/卡在哪个阶段/已读多少/重试几次；
  B. keep-alive 空闲超时 → 默认静默（属于正常回收，不该刷屏）；
  C. 同一访问再次进来 → 打印「此前已超时 N 次，这是第 M 次尝试」；
  D. 分片重试次数确实被累计到该请求上（上传侧 + 下载侧）。
"""
import contextlib
import http.client
import io
import re
import socket
import sys
import threading
import time
import types
from http.server import ThreadingHTTPServer

import tg as _tg
import webdav

# 让超时快一点：body 读超时 / 空闲超时都压到 1s 级
webdav._BODY_TIMEOUT = 1.0
IDLE = 1.0

LOGS = []
STDERR = []
_real_log = webdav._log


def _cap(msg):
    LOGS.append(msg)
    _real_log(msg)


webdav._log = _cap


class Cfg:
    idle_timeout = IDLE
    keepalive = True

    def __getattr__(self, k):      # 其余配置按需给个安全默认值
        return None


def _make_server():
    app = types.SimpleNamespace(config=Cfg(), db=None, backend=None)

    class H(webdav.WebDAVHandler):
        def do_PUT(self):
            """只做「读请求体」这一件事：真实 PUT 的其余步骤与本测试无关。"""
            n = int(self.headers.get("Content-Length") or 0)
            self._read_exact(n)
            self.send_response(201)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

    class S(ThreadingHTTPServer):
        daemon_threads = True
        allow_reuse_address = True

    srv = S(("127.0.0.1", 0), H)
    srv.app = app
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


def _drain_logs():
    out = list(LOGS)
    LOGS.clear()
    return out


def _has(logs, kw):
    return [l for l in logs if kw in l]


class _TeeErr(io.StringIO):
    """把 stderr 收下来：标准库那行裸超时日志是直接写 stderr 的。"""

    def write(self, s):
        STDERR.append(s)
        return len(s)

    def flush(self):
        pass


PASS, FAIL = [], []


def check(cond, label, detail=""):
    (PASS if cond else FAIL).append(label)
    print(f"  {'OK ' if cond else 'FAIL'} | {label}" + (f"  {detail}" if detail else ""))


def main():
    srv = _make_server()
    port = srv.server_address[1]

    # ---------------- A. 请求处理中超时 ----------------
    print("\n[A] 请求处理中超时 —— 应打印 客户端/请求/阶段/已读/重试")
    s = socket.create_connection(("127.0.0.1", port), timeout=30)
    s.sendall(b"PUT /big/movie.mkv HTTP/1.1\r\nHost: t\r\nContent-Length: 1000000\r\n\r\n")
    s.sendall(b"x" * 200000)        # 只发一部分，剩下永远不来 → 读请求体超时
    time.sleep(2.2)
    s.close()
    logs = _drain_logs()
    hit = _has(logs, "请求超时(连接已关闭")
    check(bool(hit), "A1 打印了带上下文的超时日志（不再是裸 'Request timed out'）")
    line = hit[0] if hit else ""
    print(f"       → {line}")
    check("PUT /big/movie.mkv" in line, "A2 含 method + path（哪个访问超时了）")
    check("读取请求体" in line, "A3 含卡住的阶段（读取请求体/写出响应）")
    check(re.search(r"已读请求体=[\d.]+(KB|MB).*\(\d+%\)", line) is not None,
          "A4 含已读字节数与进度百分比")
    check("重试情况=" in line, "A5 含重试情况")
    check("该访问累计超时1次" in line, "A6 含该访问的累计超时次数")
    check(not [l for l in STDERR if re.match(r"^\S+ - - \[[^]]+\] Request timed out", l)],
          "A7 标准库那行无上下文日志不再出现（stderr 已无）")

    # ---------------- B. keep-alive 空闲超时 ----------------
    print("\n[B] keep-alive 空闲等待超时 —— 属正常回收，默认静默")
    s2 = socket.create_connection(("127.0.0.1", port), timeout=30)
    s2.sendall(b"GET /a.txt HTTP/1.1\r\nHost: t\r\n\r\n")
    time.sleep(0.6)
    _drain_logs()
    s2.recv(65536)
    time.sleep(2.0)                 # 不发下一个请求 → 空等超时
    logs_b = _drain_logs()
    s2.close()
    check(not _has(logs_b, "请求超时(连接已关闭"),
          "B1 空闲回收不被误报成「请求超时」")
    check(not _has(logs_b, "Request timed out"),
          "B2 空闲回收不刷标准库那行日志")
    print(f"       → 静默，日志条数={len(logs_b)}")

    # ---------------- C. 客户端重传：能看到「第几次尝试」 ----------------
    print("\n[C] 同一访问超时后被重发 —— 应打印重传提示")
    s3 = socket.create_connection(("127.0.0.1", port), timeout=30)
    s3.sendall(b"PUT /big/movie.mkv HTTP/1.1\r\nHost: t\r\nContent-Length: 1000000\r\n\r\n")
    s3.sendall(b"y" * 2048)
    time.sleep(2.2)
    s3.close()
    logs_c = _drain_logs()
    STDERR.clear()
    hit2 = _has(logs_c, "请求重传")
    check(bool(hit2), "C1 识别出这是一次重传并打印")
    if hit2:
        print(f"       → {hit2[0]}")
        check("此前已超时 1 次" in hit2[0], "C2 说清此前超时次数")
        check("第 2 次尝试" in hit2[0], "C3 说清当前是第几次尝试")
        check("去重复用" in hit2[0], "C4 PUT 重传时提示会复用已成功分片")

    # ---------------- D. 重试次数确实累计到请求上 ----------------
    print("\n[D] 分片重试计数（超时日志里「重试了几次」的依据）")
    ctx = webdav._ReqCtx("1.2.3.4", "PUT", "/x.bin", 1)
    check("无重试(一次通过)" in ctx.retry_desc(), "D1 无重试时明确写「无重试」")
    ctx.bump_up()
    ctx.bump_up()
    ctx.bump_down()
    d = ctx.retry_desc()
    check("上传分片重试2次" in d and "下载分片重试1次" in d,
          "D2 上/下载重试分别计数", f"→ {d}")

    # 下载侧：用真实的 TelegramBackend 打桩，验证瞬断重试确实 bump 了传进去的 ctx
    import tg as _tgm
    be = _tgm.TelegramBackend(
        slots=[{"token": "T", "chat_id": "-100"}], api_base="https://example.invalid")
    be._candidates = {0: [("https://example.invalid", None)]}
    state = {"getfile": 0}

    def fake_get_file_path(file_id, token, api_base, proxy_token):
        state["getfile"] += 1
        if state["getfile"] == 1:
            raise http.client.ResponseNotReady("模拟代理瞬断")
        return "photos/f.jpg"

    def fake_do_get(api_base, path, proxy_token, rng, timeout=180, force_new=False):
        seq = [b"ab", b"cd", b""]
        resp = types.SimpleNamespace(status=200, will_close=False,
                                     read=lambda n: seq.pop(0) if seq else b"",
                                     isclosed=lambda: True)
        return (object(), resp)

    be._get_file_path = fake_get_file_path
    be._do_get = fake_do_get
    be._release_conn = lambda *a, **k: None

    c2 = webdav._ReqCtx("1.2.3.4", "GET", "/v.mp4", 1)
    got = b"".join(be.iter_chunk("F", 0, 0, 3, blk=8, ctx=c2))
    check(got == b"abcd", "D3 下载瞬断后重试成功（数据完整未重复）", f"got={got!r}")
    check(c2.down_retry == 1, "D4 下载分片重试被累计到该请求（ctx.down_retry=1）",
          f"实际={c2.down_retry}")
    check("下载分片重试1次" in c2.retry_desc(), "D5 摘要里能看到下载重试次数",
          f"→ {c2.retry_desc()}")

    srv.shutdown()

    print(f"\n结果: {len(PASS)} 通过 / {len(FAIL)} 失败")
    if FAIL:
        for f in FAIL:
            print("  FAILED:", f)
        return 1
    return 0


if __name__ == "__main__":
    with contextlib.redirect_stderr(_TeeErr()):
        sys.exit(main())
