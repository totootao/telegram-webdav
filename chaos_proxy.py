"""本地 chaos 反代：在 tg.py 与真实 Telegram 代理之间随机注入网络抖动，用于验证
「下载过程随机停止」修复后，在真实高频抖动下能否自愈且字节完整。

它监听 0.0.0.0:CHAOS_PORT，把每个请求原样转发到上游真实代理（CHAOS_UPSTREAM），
但在 连接建立/首字节前/读取中途 随机注入以下故障：
  - connfail : 不连上游，直接关闭客户端连接（模拟代理瞬断 / 连接重置）
  - delay    : 随机 sleep 后正常（模拟高延迟 / 抖动）
  - earlydrop: 连上上游、发完响应头后 0 body 立即断（首字节前的断流，可安全重试）
  - middrop  : 连上上游、转发部分 body 后断（读取中途断流，已写字节则只能干净失败）

通过环境变量调概率（默认偏向可重试故障，聚焦「自愈」验证）：
  JITTER_CONNFAIL_P  JITTER_DELAY_P  JITTER_EARLYDROP_P  JITTER_MIDDROP_P
  JITTER_MAX_DELAY   JITTER_MODE(recover|corrupt)

单独暴露 /__chaos_stats 查看注入统计。
"""
import json
import os
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = os.environ.get("CHAOS_UPSTREAM", "https://otterhub-tg-proxy-3uj.pages.dev")
PORT = int(os.environ.get("CHAOS_PORT", "18080"))
P_CONNFAIL = float(os.environ.get("JITTER_CONNFAIL_P", "0.35"))
P_DELAY = float(os.environ.get("JITTER_DELAY_P", "0.30"))
P_EARLYDROP = float(os.environ.get("JITTER_EARLYDROP_P", "0.25"))
P_MIDDROP = float(os.environ.get("JITTER_MIDDROP_P", "0.10"))
MAX_DELAY = float(os.environ.get("JITTER_MAX_DELAY", "1.2"))

stats_lock = threading.Lock()
stats = {"req": 0, "connfail": 0, "delay": 0, "earlydrop": 0, "middrop": 0, "ok": 0}


def pick_fault():
    r = random.random()
    if r < P_CONNFAIL:
        return "connfail"
    if r < P_CONNFAIL + P_DELAY:
        return "delay"
    if r < P_CONNFAIL + P_DELAY + P_EARLYDROP:
        return "earlydrop"
    if r < P_CONNFAIL + P_DELAY + P_EARLYDROP + P_MIDDROP:
        return "middrop"
    return "ok"


def _fwd_headers(h):
    out = {}
    for k, v in h.items():
        # 只过滤逐跳(hop-by-hop)头；Content-Length 必须保留，否则下游拿不到长度、
        # 无法区分「代理少发/提前 FIN」与「正常 EOF」，自愈与静默截断校验都会失效。
        if k.lower() in ("host", "connection", "transfer-encoding", "proxy-connection"):
            continue
        out[k] = v
    return out


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _proxy(self):
        with stats_lock:
            stats["req"] += 1
        target = UPSTREAM + self.path
        method = self.command
        body = None
        if method == "POST":
            length = int(self.headers.get("Content-Length", 0) or 0)
            body = self.rfile.read(length) if length else None
        headers = _fwd_headers(self.headers)
        fault = pick_fault()

        if fault == "connfail":
            with stats_lock:
                stats["connfail"] += 1
            # 模拟代理瞬断：直接断开客户端连接，不往上走
            self.close_connection = True
            try:
                self.connection.close()
            except Exception:
                pass
            return

        if fault == "delay":
            with stats_lock:
                stats["delay"] += 1
            time.sleep(random.uniform(0.05, MAX_DELAY))
            self._proxy_normal(target, method, body, headers)
            return

        if fault == "earlydrop":
            with stats_lock:
                stats["earlydrop"] += 1
            self._proxy_earlydrop(target, method, body, headers)
            return

        if fault == "middrop":
            with stats_lock:
                stats["middrop"] += 1
            self._proxy_middrop(target, method, body, headers)
            return

        with stats_lock:
            stats["ok"] += 1
        self._proxy_normal(target, method, body, headers)

    def _proxy_normal(self, target, method, body, headers):
        req = urllib.request.Request(target, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=150) as resp:
                self.send_response(resp.status)
                for k, v in resp.getheaders():
                    if k.lower() in ("transfer-encoding", "connection"):
                        continue
                    self.send_header(k, v)
                self.end_headers()
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                    except Exception:
                        break
        except urllib.error.HTTPError as e:
            try:
                self.send_response(e.code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(e.read())
            except Exception:
                pass
        except Exception:
            try:
                self.connection.close()
            except Exception:
                pass

    def _proxy_earlydrop(self, target, method, body, headers):
        # 连上上游、发出响应头，但 0 body 立即断流：首字节前的断流，可安全重试
        req = urllib.request.Request(target, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=150) as resp:
                self.send_response(resp.status)
                for k, v in resp.getheaders():
                    if k.lower() in ("transfer-encoding", "connection"):
                        continue
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.flush()
                # 不发任何 body，直接断开（保留上游 Content-Length 以便下游读到长度不符而异常重试）
                raise BrokenPipeError("chaos earlydrop")
        except (BrokenPipeError, ConnectionResetError, OSError):
            try:
                self.connection.close()
            except Exception:
                pass

    def _proxy_middrop(self, target, method, body, headers):
        # 连上上游、转发一部分 body 后断流：读取中途断流（已写字节则只能干净失败）
        req = urllib.request.Request(target, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=150) as resp:
                self.send_response(resp.status)
                for k, v in resp.getheaders():
                    if k.lower() in ("transfer-encoding", "connection"):
                        continue
                    self.send_header(k, v)
                self.end_headers()
                total = 0
                limit = random.randint(256 * 1024, 6 * 1024 * 1024)
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except Exception:
                        return
                    total += len(chunk)
                    if total >= limit:
                        break
                raise BrokenPipeError("chaos middrop")
        except (BrokenPipeError, ConnectionResetError, OSError):
            try:
                self.connection.close()
            except Exception:
                pass

    def do_GET(self):
        if self.path == "/__chaos_stats":
            body = json.dumps(dict(stats)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._proxy()

    def do_POST(self):
        self._proxy()


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"chaos proxy on :{PORT} -> {UPSTREAM} "
          f"(connfail={P_CONNFAIL} delay={P_DELAY} earlydrop={P_EARLYDROP} middrop={P_MIDDROP})",
          flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
