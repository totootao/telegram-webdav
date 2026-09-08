#!/usr/bin/env python3
"""端到端自测：启动假 Telegram + WebDAV 服务，用标准库客户端跑完整 WebDAV 流程。

覆盖：认证 / PROPFIND / MKCOL / PUT(小文件单分片) / PUT(大文件多分片) /
      GET 全量 / GET Range(头/中/尾) / HEAD / MOVE / COPY / DELETE / 空文件 / webhook 入库。

运行：python3 selftest.py
"""
import base64
import hashlib
import json
import os
import socket
import sys
import threading
import time
import urllib.request

# ---------- 选空闲端口 ----------
def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


FAKE_PORT = _free_port()
DAV_PORT = _free_port()
DB_PATH = os.path.join("/tmp", f"tgwebdav_selftest_{os.getpid()}.db")

os.environ["TG_API_BASE"] = f"http://127.0.0.1:{FAKE_PORT}"
os.environ["TG_BOT_TOKEN"] = "FAKE_TOKEN"
os.environ["TG_CHAT_ID"] = "-100FAKE"
os.environ["CHUNK_SIZE_MB"] = "20"
os.environ["DB_PATH"] = DB_PATH
os.environ["PORT"] = str(DAV_PORT)
os.environ["HOST"] = "127.0.0.1"
os.environ["DAV_USER"] = "tester"
os.environ["DAV_PASSWORD"] = "s3cr3t"
os.environ["TG_WEBHOOK_SECRET"] = "sekret"
os.environ["WEBDAV_IMPORT_DIR"] = "/telegram-import"

# 必须在 import server 前设好环境变量（config 在导入时读取）
import fake_telegram as ftg
from server import make_server
import db as dbs

ftg.start_fake(FAKE_PORT)
srv = make_server()
t = threading.Thread(target=srv.serve_forever, daemon=True)
t.start()
time.sleep(0.3)

BASE = f"http://127.0.0.1:{DAV_PORT}"
AUTH = "Basic " + base64.b64encode(b"tester:s3cr3t").decode()


def req(method, path, body=None, headers=None, raw=False):
    url = BASE + path
    r = urllib.request.Request(url, data=body, method=method)
    if headers:
        for k, v in headers.items():
            r.add_header(k, v)
    r.add_header("Authorization", AUTH)
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            data = resp.read()
            return resp.status, dict(resp.headers), data
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


results = []
def check(name, cond, extra=""):
    results.append((name, cond, extra))
    print(("PASS " if cond else "FAIL ") + name + (("  -> " + extra) if extra and not cond else ""))


# 1) 未带认证应 401
r = urllib.request.Request(BASE + "/", method="PROPFIND")
try:
    urllib.request.urlopen(r, timeout=5).read()
    st = 200
except urllib.error.HTTPError as e:
    st = e.code
check("auth.required(401)", st == 401, f"status={st}")

# 2) PROPFIND 根
st, h, b = req("PROPFIND", "/", body=b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>', headers={"Depth": "1"})
check("propfind.root(207)", st == 207, f"status={st}")

# 3) MKCOL
st, h, b = req("MKCOL", "/test")
check("mkcol(201)", st == 201, f"status={st}")

# 4) PUT 小文件（5MB -> 单分片）
small = os.urandom(5 * 1024 * 1024)
st, h, b = req("PUT", "/test/small.bin", body=small, headers={"Content-Type": "application/octet-stream"})
check("put.small(201)", st == 201, f"status={st}")
node = dbs.MetaStore(DB_PATH).get_node("/test/small.bin")
chk = json.loads(node["chunks"]) if node and node.get("chunks") else []
check("put.small.single_chunk", node is not None and len(chk) == 1, f"chunks={len(chk)}")
check("put.small.size", node is not None and node["size"] == len(small), f"size={node['size'] if node else None}")

# 5) PUT 大文件（45MB -> 多分片）
big = os.urandom(45 * 1024 * 1024)
st, h, b = req("PUT", "/test/big.bin", body=big, headers={"Content-Type": "application/octet-stream"})
check("put.big(201)", st == 201, f"status={st}")
meta = dbs.MetaStore(DB_PATH).get_node("/test/big.bin")
chunks = json.loads(meta["chunks"]) if meta and meta.get("chunks") else []
check("put.big.multi_chunk", len(chunks) >= 2, f"chunks={len(chunks)}")
check("put.big.size", meta is not None and meta["size"] == len(big), f"size={meta['size'] if meta else None}")
check("put.big.chunk_sum", sum(c["size"] for c in chunks) == len(big), f"sum={sum(c['size'] for c in chunks)}")

# 6) GET 全量 + sha 一致
st, h, b = req("GET", "/test/big.bin")
check("get.full(200)", st == 200, f"status={st}, len={len(b)}")
check("get.full.sha", st == 200 and hashlib.sha256(b).hexdigest() == hashlib.sha256(big).hexdigest(),
      "" if st == 200 and hashlib.sha256(b).hexdigest() == hashlib.sha256(big).hexdigest() else "sha mismatch")

# 7) GET Range 头 100 字节
st, h, b = req("GET", "/test/big.bin", headers={"Range": "bytes=0-99"})
check("get.range.head(206)", st == 206 and b == big[0:100], f"status={st}, len={len(b)}")

# 8) GET Range 中段（跨分片：第 1 块尾 + 第 2 块头）
a, c = 20 * 1024 * 1024 - 50, 20 * 1024 * 1024 + 49  # 跨 20MB 边界
st, h, b = req("GET", "/test/big.bin", headers={"Range": f"bytes={a}-{c}"})
check("get.range.mid_cross_chunk(206)", st == 206 and b == big[a:c + 1], f"status={st}, len={len(b)}")

# 9) HEAD 头
st, h, b = req("HEAD", "/test/big.bin")
check("head.content_length", st == 200 and h.get("Content-Length") == str(len(big)), f"cl={h.get('Content-Length')}")

# 10) MOVE
st, h, b = req("MOVE", "/test/big.bin", headers={"Destination": BASE + "/test/moved.bin", "Overwrite": "T"})
check("move(204)", st in (201, 204), f"status={st}")
check("move.src_gone", dbs.MetaStore(DB_PATH).get_node("/test/big.bin") is None)
check("move.dst_present", dbs.MetaStore(DB_PATH).get_node("/test/moved.bin") is not None)

# 11) COPY
st, h, b = req("COPY", "/test/small.bin", headers={"Destination": BASE + "/test/copied.bin", "Overwrite": "T"})
check("copy(201)", st in (201, 204), f"status={st}")
st, h, b2 = req("GET", "/test/copied.bin")
check("copy.content_equal", b2 == small, f"len={len(b2)}")

# 12) 空文件
st, h, b = req("PUT", "/test/empty.bin", body=b"", headers={"Content-Type": "application/octet-stream"})
check("put.empty(201)", st == 201, f"status={st}")
st, h, b = req("GET", "/test/empty.bin")
check("get.empty(200,0)", st == 200 and len(b) == 0, f"status={st}, len={len(b)}")

# 13) DELETE
st, h, b = req("DELETE", "/test/small.bin")
check("delete(204)", st == 204, f"status={st}")
check("delete.gone", dbs.MetaStore(DB_PATH).get_node("/test/small.bin") is None)

# 14) webhook 入库
ftg.STORE["FAKE-WEBHOOK1"] = b"hello from channel"
upd = {
    "update_id": 1,
    "message": {
        "message_id": 777,
        "document": {"file_id": "FAKE-WEBHOOK1", "file_name": "chan.txt",
                     "file_size": 18, "mime_type": "text/plain"},
    },
}
st, h, b = req("POST", "/telegram/webhook", body=json.dumps(upd).encode(),
               headers={"Content-Type": "application/json", "X-Telegram-Bot-Api-Secret-Token": "sekret"})
check("webhook.import(200)", st == 200, f"status={st}")
wn = dbs.MetaStore(DB_PATH).get_node("/telegram-import/chan.txt")
check("webhook.node_present", wn is not None, f"node={wn}")
if wn:
    st, h, b = req("GET", "/telegram-import/chan.txt")
    check("webhook.content", b == b"hello from channel", f"got={b!r}")

# 15) PROPFIND 子目录列举
st, h, b = req("PROPFIND", "/test", body=b'<D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>', headers={"Depth": "1"})
check("propfind.depth1(207)", st == 207, f"status={st}")

srv.shutdown()
passed = sum(1 for _, c, _ in results if c)
total = len(results)
print("\n" + "=" * 40)
print(f"结果: {passed}/{total} 通过")
for name, c, extra in results:
    if not c:
        print("  FAIL:", name, extra)
sys.exit(0 if passed == total else 1)
