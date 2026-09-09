"""精准验证「只有失败的分片会被重新上传」。

模拟：一个 100MB 文件切成 5 片，假设上次上传时**第 2、4 片失败**（其余 3 片成功）。
做法：把去重表里第 2、4 片的记录删掉（等价于那两片没传成功），然后重传整个文件。
预期：服务端只真正上传 2 片，其余 3 片命中去重跳过——即「只补传失败的分片」。
"""
import base64
import hashlib
import http.client
import os
import sqlite3
import subprocess
import time

HOST, PORT = "127.0.0.1", 10010
AUTH = base64.b64encode(b"totootao:Hhangxing963.").decode()
LOCAL = "/tmp/dedup_100mb.bin"
REMOTE = "/deduptest/t.bin"
CHUNK = 20 * 1024 * 1024
DB = "/home/docker/data/tg-webdav/telegram_webdav.db"
FAIL_IDX = [2, 4]  # 模拟这两片上次上传失败


def chunk_shas():
    shas = []
    with open(LOCAL, "rb") as f:
        while True:
            b = f.read(CHUNK)
            if not b:
                break
            shas.append(hashlib.sha256(b).hexdigest())
    return shas


def drop_records(shas):
    """删掉指定分片的去重记录 = 模拟那些分片上次没传成功。"""
    con = sqlite3.connect(DB, timeout=30)
    n = 0
    for i in FAIL_IDX:
        cur = con.execute("DELETE FROM chunk_dedup WHERE sha=?", (shas[i],))
        n += cur.rowcount
    con.commit()
    total = con.execute("SELECT COUNT(*) FROM chunk_dedup").fetchone()[0]
    con.close()
    return n, total


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


def file_sha():
    h = hashlib.sha256()
    with open(LOCAL, "rb") as f:
        while True:
            b = f.read(8 * 1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def main():
    if not os.path.exists(LOCAL):
        print("请先运行 dedup_test.py 生成测试文件")
        return
    shas = chunk_shas()
    print(f"文件分片数: {len(shas)}")
    deleted, remain = drop_records(shas)
    print(f"已删除分片 {FAIL_IDX} 的去重记录 {deleted} 条（模拟它们上次上传失败）")
    print(f"去重表剩余 {remain} 条（即其余分片已成功）")

    print("\n=== 重传整个文件（模拟客户端失败重传行为）===")
    st, dt = upload()
    print(f"  status={st} 耗时={dt:.2f}s")

    n, sha = download_sha()
    src = file_sha()
    print(f"\n=== 校验 ===\n  下载 {n}B SHA一致={sha == src}")

    print("\n=== 服务端日志（本次上传实际传了几片）===")
    out = subprocess.run(
        ["docker", "logs", "tg-webdav", "--since", "1m"],
        capture_output=True, text=True).stdout
    for line in out.splitlines():
        if "复用统计" in line or "命中去重" in line:
            print("  " + line.split("][webdav] ")[-1])


if __name__ == "__main__":
    main()
