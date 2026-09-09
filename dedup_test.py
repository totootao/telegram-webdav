"""验证「分片重试 + 重传时只补传失败分片（按 SHA 去重）」。

测试思路：
  1. 生成 100MB 全新随机文件（5 个分片内容各不相同，走流式上传 >32MB）
  2. 第一次 PUT：5 片全部真实上传 Telegram
  3. 第二次 PUT 同一路径（模拟「上传失败后客户端重传」）：
     5 片应全部命中去重 → 跳过 Telegram 上传 → 耗时应显著小于第一次
  4. 下载校验 SHA-256，确认复用 file_id 后内容依然正确
"""
import base64
import hashlib
import http.client
import os
import time

HOST, PORT = "127.0.0.1", 10010
AUTH = base64.b64encode(b"totootao:Hhangxing963.").decode()
LOCAL = "/tmp/dedup_100mb.bin"
REMOTE = "/deduptest/t.bin"
SIZE = 100 * 1024 * 1024


def make_file():
    if os.path.exists(LOCAL) and os.path.getsize(LOCAL) == SIZE:
        print(f"复用已有测试文件 {LOCAL}")
        return
    print("生成 100MB 随机文件...")
    with open(LOCAL, "wb") as f:
        w = 0
        while w < SIZE:
            b = os.urandom(4 * 1024 * 1024)
            f.write(b)
            w += len(b)
    print("生成完成")


def file_sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while True:
            b = f.read(8 * 1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def req(method, path, body=None, headers=None):
    c = http.client.HTTPConnection(HOST, PORT, timeout=900)
    h = {"Authorization": f"Basic {AUTH}"}
    h.update(headers or {})
    c.request(method, path, body=body, headers=h)
    r = c.getresponse()
    d = r.read()
    st = r.status
    c.close()
    return st, d


def upload():
    size = os.path.getsize(LOCAL)
    c = http.client.HTTPConnection(HOST, PORT, timeout=900)
    c.putrequest("PUT", REMOTE)
    c.putheader("Authorization", f"Basic {AUTH}")
    c.putheader("Content-Length", str(size))
    c.endheaders()
    t0 = time.time()
    with open(LOCAL, "rb") as f:
        while True:
            b = f.read(4 * 1024 * 1024)
            if not b:
                break
            c.send(b)
    r = c.getresponse()
    st = r.status
    r.read()
    c.close()
    return st, time.time() - t0


def download_sha():
    c = http.client.HTTPConnection(HOST, PORT, timeout=900)
    c.putrequest("GET", REMOTE)
    c.putheader("Authorization", f"Basic {AUTH}")
    c.endheaders()
    r = c.getresponse()
    h = hashlib.sha256()
    n = 0
    while True:
        b = r.read(256 * 1024)
        if not b:
            break
        n += len(b)
        h.update(b)
    c.close()
    return n, h.hexdigest()


def main():
    make_file()
    src = file_sha(LOCAL)
    print(f"本地 SHA: {src[:16]}...")
    req("MKCOL", "/deduptest")

    print("\n=== 第 1 次上传（5 片全部真实上传 Telegram）===")
    st1, dt1 = upload()
    print(f"  status={st1} 耗时={dt1:.2f}s")

    print("\n=== 第 2 次上传同一路径（模拟失败后重传）===")
    st2, dt2 = upload()
    print(f"  status={st2} 耗时={dt2:.2f}s")
    print(f"  提速: {dt1 / dt2:.1f}x" if dt2 > 0 else "  (dt2=0)")

    print("\n=== 下载校验 ===")
    n, sha = download_sha()
    print(f"  下载字节={n} SHA={sha[:16]}...")
    print(f"  SHA 一致: {sha == src}")

    print("\n=== 结论 ===")
    ok_dedup = dt2 < dt1 * 0.5
    print(f"  第2次显著更快(去重生效): {ok_dedup}  ({dt1:.2f}s -> {dt2:.2f}s)")
    print(f"  内容正确(SHA一致): {sha == src}")
    print(f"  上传状态: {st1} / {st2}")


if __name__ == "__main__":
    main()
