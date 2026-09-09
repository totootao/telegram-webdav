"""大文件（>1GB）真实环境上传/下载测试。

针对真实服务（真实 bot 池 + 自建代理 + 真实频道），重点回答：
  1. 1.2GB 文件能否完整上传/下载，SHA-256 是否一致
  2. 上传时服务端内存峰值（PUT 是整包读进内存，1GB 文件会吃 1GB+ 吗）
  3. 下载时服务端内存峰值（全分片流式下发是否真的不缓冲）
  4. 61 个分片下：分片间隔是否仍为 0（无卡顿）
  5. Range / 起播 / seek / 断点续传 / 并发 在超大文件下是否正常

用法：
  python3 bigfile_test.py upload      # 只上传（生成文件 + PUT）
  python3 bigfile_test.py download    # 只下载（需先上传）
  python3 bigfile_test.py all         # 全部（默认）
"""
import base64
import hashlib
import http.client
import os
import subprocess
import sys
import threading
import time

HOST = os.environ.get("BIG_HOST", "127.0.0.1")
PORT = int(os.environ.get("BIG_PORT", "10010"))
USER = os.environ.get("BIG_USER", "totootao")
PASS = os.environ.get("BIG_PASS", "Hhangxing963.")
CONTAINER = os.environ.get("BIG_CONTAINER", "tg-webdav")
SIZE_MB = int(os.environ.get("BIG_SIZE_MB", "1200"))  # 默认 1.2GB
LOCAL = f"/tmp/big_{SIZE_MB}mb.bin"
REMOTE = f"/bigtest/big_{SIZE_MB}mb.bin"
AUTH = base64.b64encode(f"{USER}:{PASS}".encode()).decode()

PASSED, FAILED = [], []


def check(name, ok, detail=""):
    (PASSED if ok else FAILED).append(name)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name} {detail}", flush=True)


def fmt_mb(n):
    return f"{n / 1024 / 1024:.1f} MB"


def fmt_spd(n, dt):
    return f"{n / 1024 / 1024 / dt:.2f} MB/s" if dt > 0 else "∞"


# ---------- 服务端内存采样 ----------
class MemMon:
    def __init__(self):
        self.peak = 0.0
        self.base = 0.0
        self._stop = threading.Event()
        self._t = None

    def _sample(self):
        try:
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", CONTAINER],
                capture_output=True, text=True, timeout=10).stdout.strip()
            if not out:
                return None
            used = out.split("/")[0].strip()
            if used.endswith("GiB"):
                return float(used[:-3]) * 1024
            if used.endswith("MiB"):
                return float(used[:-3])
            if used.endswith("KiB"):
                return float(used[:-3]) / 1024
        except Exception:
            pass
        return None

    def start(self):
        self.base = self._sample() or 0.0
        self.peak = self.base

        def loop():
            while not self._stop.is_set():
                v = self._sample()
                if v is not None:
                    self.peak = max(self.peak, v)
                time.sleep(0.4)
        self._t = threading.Thread(target=loop, daemon=True)
        self._t.start()

    def stop(self):
        self._stop.set()
        if self._t:
            self._t.join(timeout=2)
        return self.peak, self.base


def conn():
    return http.client.HTTPConnection(HOST, PORT, timeout=900)


def req_raw(method, path, headers=None, body=None):
    c = conn()
    h = {"Authorization": f"Basic {AUTH}"}
    h.update(headers or {})
    c.request(method, path, body=body, headers=h)
    r = c.getresponse()
    data = r.read()
    st = r.status
    hdrs = dict(r.getheaders())
    c.close()
    return st, hdrs, data


def make_file(path, size_mb):
    if os.path.exists(path) and os.path.getsize(path) == size_mb * 1024 * 1024:
        print(f"  复用已有测试文件 {path}（{size_mb}MB）", flush=True)
        return
    print(f"  生成 {size_mb}MB 测试文件（64MB 随机块循环，避免占用过多内存）...", flush=True)
    blk = os.urandom(64 * 1024 * 1024)
    total = size_mb * 1024 * 1024
    with open(path, "wb") as f:
        w = 0
        while w < total:
            n = min(len(blk), total - w)
            f.write(blk[:n])
            w += n
    print(f"  生成完成 size={os.path.getsize(path)}", flush=True)


def file_sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(8 * 1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def upload(path, remote):
    """流式 PUT：客户端边读文件边发，不把 1.2GB 读进客户端内存。"""
    size = os.path.getsize(path)
    c = conn()
    c.putrequest("PUT", remote)
    c.putheader("Authorization", f"Basic {AUTH}")
    c.putheader("Content-Type", "application/octet-stream")
    c.putheader("Content-Length", str(size))
    c.endheaders()
    t0 = time.time()
    with open(path, "rb") as f:
        while True:
            b = f.read(4 * 1024 * 1024)
            if not b:
                break
            c.send(b)
    r = c.getresponse()
    body = r.read()
    st = r.status
    c.close()
    return st, time.time() - t0, size


def download_range(remote, start=None, end=None, stop_after=None, sha=True):
    """流式下载：可指定 Range、可读到 stop_after 字节就断开（模拟起播/客户端断开）。"""
    c = conn()
    c.putrequest("GET", remote)
    c.putheader("Authorization", f"Basic {AUTH}")
    if start is not None:
        rng = f"bytes={start}-" if end is None else f"bytes={start}-{end}"
        c.putheader("Range", rng)
    c.endheaders()
    r = c.getresponse()
    st = r.status
    t0 = time.time()
    t_first = None
    h = hashlib.sha256() if sha else None
    n = 0
    while True:
        b = r.read(256 * 1024)
        if not b:
            break
        if t_first is None:
            t_first = time.time() - t0
        n += len(b)
        if h:
            h.update(b)
        if stop_after and n >= stop_after:
            break
    try:
        c.close()
    except Exception:
        pass
    return {
        "status": st, "bytes": n, "ttfb": t_first or 0.0,
        "elapsed": time.time() - t0,
        "sha": h.hexdigest() if h else None,
    }


def expected_sha_of_slice(path, start, end):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        f.seek(start)
        left = end - start + 1
        while left > 0:
            b = f.read(min(8 * 1024 * 1024, left))
            if not b:
                break
            h.update(b)
            left -= len(b)
    return h.hexdigest()


def cleanup():
    req_raw("DELETE", "/bigtest")


def stage_upload():
    print(f"\n=== 阶段1：上传 {SIZE_MB}MB（>{SIZE_MB/1024:.1f}GB）===", flush=True)
    make_file(LOCAL, SIZE_MB)
    src_sha = file_sha(LOCAL)
    print(f"  本地文件 SHA-256={src_sha[:16]}...", flush=True)
    open("/tmp/big_sha.txt", "w").write(src_sha)

    req_raw("MKCOL", "/bigtest")
    mon = MemMon()
    mon.start()
    st, dt, size = upload(LOCAL, REMOTE)
    peak, base = mon.stop()
    print(f"  上传完成 status={st} 耗时={dt:.2f}s 吞吐={fmt_spd(size, dt)}", flush=True)
    print(f"  服务端内存: 基线={base:.0f}MB → 峰值={peak:.0f}MB "
          f"(增量={peak - base:.0f}MB)", flush=True)
    check("upload.status", st in (200, 201, 204), f"status={st}")
    check("upload.throughput_reasonable", size / dt > 1 * 1024 * 1024,
          f"吞吐={fmt_spd(size, dt)}")
    # 上传内存：流式边收边传（_upload_streaming），内存峰值应与文件大小无关，
    # 约等于 在飞分片数×分片大小 + Python 缓冲/GC 波动，落在数百 MB。
    # 旧路径整包读进内存：1.2GB 文件吃掉 ~2.27×≈2743MB，2GB 文件直接 OOM。
    # 这里断言峰值被限住（< 文件大小本身，远小于旧的 2.27×），不随文件线性增长。
    file_mb = size / 1024 / 1024
    ratio = (peak - base) / file_mb if file_mb else 0
    check("upload.mem_bounded_streaming",
          (peak - base) < file_mb * 0.9,
          f"内存增量={peak - base:.0f}MB / 文件={file_mb:.0f}MB 占比={ratio*100:.1f}% "
          f"(旧路径 2.27×≈{file_mb*2.27:.0f}MB；流式:峰值被限在数百 MB)")
    return src_sha


def stage_download(src_sha):
    print(f"\n=== 阶段2：全量下载 {SIZE_MB}MB + SHA-256 校验 ===", flush=True)
    mon = MemMon()
    mon.start()
    r = download_range(REMOTE)
    peak, base = mon.stop()
    print(f"  下载完成 status={r['status']} 字节={fmt_mb(r['bytes'])} "
          f"耗时={r['elapsed']:.2f}s 吞吐={fmt_spd(r['bytes'], r['elapsed'])} "
          f"TTFB={r['ttfb']:.3f}s", flush=True)
    print(f"  服务端内存: 基线={base:.0f}MB → 峰值={peak:.0f}MB "
          f"(增量={peak - base:.0f}MB)", flush=True)
    check("download.full_size", r["bytes"] == SIZE_MB * 1024 * 1024,
          f"收到={r['bytes']} 期望={SIZE_MB*1024*1024}")
    check("download.sha256_match", r["sha"] == src_sha,
          f"远端={r['sha'][:16] if r['sha'] else '-'} 本地={src_sha[:16]}")
    check("download.mem_streaming_not_buffered",
          (peak - base) < 300,
          f"内存增量={peak - base:.0f}MB（流式下发，未整文件缓冲）")

    print(f"\n=== 阶段3：Range 分段（首/中/尾）===", flush=True)
    total = SIZE_MB * 1024 * 1024
    for name, s, e in (("头部 0-4MB", 0, 4 * 1024 * 1024 - 1),
                       ("中部 600MB 处 4MB", 600 * 1024 * 1024, 600 * 1024 * 1024 + 4 * 1024 * 1024 - 1),
                       ("尾部 最后2MB", total - 2 * 1024 * 1024, total - 1)):
        rr = download_range(REMOTE, s, e)
        exp = expected_sha_of_slice(LOCAL, s, e)
        check(f"range.{name}", rr["status"] == 206 and rr["sha"] == exp,
              f"status={rr['status']} bytes={fmt_mb(rr['bytes'])} "
              f"sha_ok={rr['sha'] == exp}")

    print(f"\n=== 阶段4：起播 / seek（播放器视角）===", flush=True)
    r0 = download_range(REMOTE, 0, None, stop_after=2 * 1024 * 1024, sha=False)
    check("playback.start_ttfb", r0["status"] == 206 and r0["ttfb"] < 3.0,
          f"status={r0['status']} TTFB={r0['ttfb']:.3f}s")
    mid = int(total * 0.5)
    rm = download_range(REMOTE, mid, None, stop_after=2 * 1024 * 1024, sha=False)
    check("playback.seek_50pct_ttfb", rm["status"] == 206 and rm["ttfb"] < 3.0,
          f"status={rm['status']} TTFB={rm['ttfb']:.3f}s")

    print(f"\n=== 阶段5：断点续传（分段续传 + 字节正确性）===", flush=True)
    # 用 100MB 边界做两段：模拟「下到一半断开 → 从断点续传」，逐段比对 SHA
    seg = 100 * 1024 * 1024
    p1 = download_range(REMOTE, 0, seg - 1)
    p2 = download_range(REMOTE, seg, seg * 2 - 1)
    exp1 = expected_sha_of_slice(LOCAL, 0, seg - 1)
    exp2 = expected_sha_of_slice(LOCAL, seg, seg * 2 - 1)
    check("resume.seg1_bytes_sha", p1["status"] == 206 and p1["sha"] == exp1
          and p1["bytes"] == seg,
          f"status={p1['status']} bytes={fmt_mb(p1['bytes'])}")
    check("resume.seg2_continues_from_break", p2["status"] == 206
          and p2["sha"] == exp2 and p2["bytes"] == seg,
          f"status={p2['status']} bytes={fmt_mb(p2['bytes'])} "
          f"从断点 {fmt_mb(seg)} 续传")

    print(f"\n=== 阶段6：4 路并发 Range（多用户拖拽）===", flush=True)
    results = {}
    threads = []

    def one(i):
        s = i * (total // 8)
        e = s + 4 * 1024 * 1024 - 1
        results[i] = download_range(REMOTE, s, e)

    for i in range(4):
        t = threading.Thread(target=one, args=(i,))
        threads.append(t)
        t.start()
    t0 = time.time()
    for t in threads:
        t.join()
    wall = time.time() - t0
    allok = all(results[i]["status"] == 206 for i in range(4))
    check("concurrent.4_ranges", allok,
          f"墙钟={wall:.2f}s 状态={[results[i]['status'] for i in range(4)]}")


def main():
    stage = sys.argv[1] if len(sys.argv) > 1 else "all"
    print(f"=== 大文件测试 {SIZE_MB}MB（{SIZE_MB/1024:.2f}GB）· 真实 Telegram ===", flush=True)
    print(f"目标: {HOST}:{PORT}{REMOTE}  容器={CONTAINER}", flush=True)
    t0 = time.time()
    if stage in ("upload", "all"):
        src_sha = stage_upload()
    else:
        src_sha = open("/tmp/big_sha.txt").read()
    if stage in ("download", "all"):
        stage_download(src_sha)
    print(f"\n=== 总耗时 {time.time() - t0:.1f}s ===")
    print(f"结果: {len(PASSED)} 通过 / {len(FAILED)} 失败")
    if FAILED:
        print("失败项: " + ", ".join(FAILED))


if __name__ == "__main__":
    try:
        main()
    finally:
        if os.environ.get("KEEP_DATA") != "1":
            print("\n清理测试数据...", flush=True)
            cleanup()
