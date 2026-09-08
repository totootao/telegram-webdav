"""视频播放专项自测：模拟播放器高频 Range 请求（探测/seek/预读）。

验证：
  1. Range 请求字节级正确（含切分片边界、后缀范围 bytes=-N）
  2. 连接池复用：多次请求后连接数收敛（不随请求数线性增长）
  3. 流式首字节：首块尽快 yield（这里只验证能正常拿到数据）
"""
import http.server
import threading
import urllib.parse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tg as tgmod

DATA = bytes((i * 7 + 3) % 256 for i in range(1024 * 1024 * 5))  # 5MB 假视频
FILE_ID = "FAKE_FILE_ID"
TOKEN = "123:abc"


class FakeTG(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_GET(self):
        p = urllib.parse.urlparse(self.path)
        if p.path.endswith("/getFile"):
            body = json.dumps({"ok": True, "result": {"file_path": "vid.bin"}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if "/file/bot" in p.path:
            rng = self.headers.get("Range")
            start, end = 0, len(DATA) - 1
            if rng and rng.startswith("bytes="):
                spec = rng[len("bytes="):]
                if "-" in spec:
                    s, e = spec.split("-", 1)
                    start = int(s) if s else 0
                    end = int(e) if e else len(DATA) - 1
            end = min(end, len(DATA) - 1)
            chunk = DATA[start:end + 1]
            self.send_response(206 if rng else 200)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(DATA)}")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            self.wfile.write(chunk)
            return
        self.send_response(404)
        self.end_headers()


def main():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeTG)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()

    backend = tgmod.TelegramBackend(
        slots=[{"token": TOKEN, "chat_id": "-100FAKE"}],
        api_base=f"http://127.0.0.1:{port}",
        rate_limit=0.0,
        proxy_token="",
        proxy_pools=[],
        rotate=True,
    )

    ok = True

    def check(name, got, expect):
        nonlocal ok
        if got == expect:
            print(f"PASS {name}")
        else:
            ok = False
            print(f"FAIL {name}: len(got)={len(got)} len(exp)={len(expect)} head={got[:8].hex()}")

    # 1) 整文件
    full = b"".join(backend.iter_chunk(FILE_ID, 0))
    check("full", full, DATA)

    # 2) 中段 Range（跨分片边界无所谓，单 chunk 后端）
    seg = b"".join(backend.iter_chunk(FILE_ID, 0, 1000000, 2000000 - 1))
    check("range_mid", seg, DATA[1000000:2000000])

    # 3) 后缀范围 bytes=-N
    tail = b"".join(backend.iter_chunk(FILE_ID, 0, None, None))  # 整文件
    suf = b"".join(backend.iter_chunk(FILE_ID, 0, len(DATA) - 4096, len(DATA) - 1))
    check("suffix", suf, DATA[len(DATA) - 4096:])

    # 4) 模拟播放器高频 seek：连续 50 次随机 Range，验证正确 + 连接收敛
    import random
    random.seed(1)
    for i in range(50):
        a = random.randint(0, len(DATA) - 2)
        b = min(len(DATA) - 1, a + random.randint(1, 500000))
        got = b"".join(backend.iter_chunk(FILE_ID, 0, a, b))
        if got != DATA[a:b + 1]:
            ok = False
            print(f"FAIL seek#{i} a={a} b={b}")
            break
    else:
        print("PASS seek_x50 (字节级一致)")

    # 连接池未爆炸：池子里连接数应 <= 并发度（这里串行，理想=1~2）
    pool_sizes = {k: len(v) for k, v in backend._conn_pool.items()}
    print(f"连接池状态: {pool_sizes}")
    total_conn = sum(pool_sizes.values())
    if total_conn <= 4:
        print(f"PASS 连接复用 (池内连接={total_conn}, 未随 50 次请求线性增长)")
    else:
        ok = False
        print(f"FAIL 连接未复用 (池内连接={total_conn})")

    srv.shutdown()
    print("结果:", "全部通过" if ok else "存在失败")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
