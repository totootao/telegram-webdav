"""seek TTFB 诊断：定位「中段 TTFB 偏高」的钱花在哪。

思路：
  1. 对 60MB(3×20MB 分片) 文件，分别 seek 到「分片边界」与「分片中间」共 6 个位置；
  2. 每个位置只读 1MB 就断开（模拟播放器起播），记录客户端侧：
       hdr（206 响应头返回耗时） / ttfb（首块到达） / 1MB 耗时；
  3. 每个位置连测 2 轮，区分冷(首次) / 热(file_path 已缓存)；
  4. 抓服务端新增日志，把耗时拆成：getFile 命中与否 + iter_chunk 首字节 + 总耗时；
  5. 额外做「直连代理基线」：用日志里的 path 直接 curl 代理，测同样 Range 的首字节，
     用来区分「服务端开销」还是「代理/TG 固有延迟」。

用法：python3 seek_diag.py
输出：stdout 表格 + SEEK_DIAG.md
"""
import base64
import http.client
import json
import re
import subprocess
import time

HOST, PORT = "127.0.0.1", 10010
AUTH = base64.b64encode(b"totootao:Hhangxing963.").decode()
PATH = "/prodtest/big.bin"
MB = 1024 * 1024
READ = 1 * MB
COOL = 1.0
ROUNDS = int(__import__("os").environ.get("SEEK_ROUNDS", "3"))
CONTAINER = "tgwd-seek"

POINTS = [
    ("片0起点(0MB)", 0),
    ("片0中间(10MB)", 10 * MB),
    ("片1起点(20MB)", 20 * MB),
    ("片1中间(30MB)", 30 * MB),
    ("片2起点(40MB)", 40 * MB),
    ("片2中间(50MB)", 50 * MB),
]


def logs(n=80):
    out = subprocess.run(["docker", "logs", "--tail", str(n), CONTAINER],
                         capture_output=True, text=True)
    txt = out.stdout + out.stderr
    return txt.splitlines()


def new_logs(_before_n=None):
    """取最近 80 行日志（诊断是串行请求，取尾部即可定位本次请求的日志）。"""
    return logs(80)


def seek_once(off):
    c = http.client.HTTPConnection(HOST, PORT, timeout=120)
    t0 = time.time()
    c.request("GET", PATH, headers={
        "Authorization": f"Basic {AUTH}",
        "Range": f"bytes={off}-",
    })
    r = c.getresponse()
    t_hdr = time.time() - t0
    ttfb = None
    got = 0
    while got < READ:
        b = r.read(256 * 1024)
        if not b:
            break
        if ttfb is None:
            ttfb = time.time() - t0
        got += len(b)
    dt = time.time() - t0
    c.close()
    return t_hdr, (ttfb or dt), dt, r.status


def parse_srv(ls):
    """从服务端新增日志里提取关键耗时。"""
    info = {"gf": "-", "first": "-", "total": "-", "path": ""}
    for l in ls:
        if "getFile 命中缓存" in l and info["gf"] == "-":
            info["gf"] = "命中"
        elif "getFile 请求" in l and info["gf"] == "-":
            info["gf"] = "请求"
        if "首字节" in l and ("首字节=" in l or "首字节延迟=" in l):
            m = re.search(r"首字节(?:延迟)?=([\d.]+)s", l)
            if m:
                info["first"] = m.group(1) + "s"
            m2 = re.search(r"总耗时=([\d.]+)s", l)
            if m2:
                info["total"] = m2.group(1) + "s"
        m3 = re.search(r"path=(/file/bot[^ ]+)", l)
        if m3 and not info["path"]:
            info["path"] = m3.group(1)
    return info


def main():
    rows = []
    print(f"{'位置':<15}{'轮':<3}{'206头':>8}{'首块':>8}{'1MB':>8}  "
          f"{'getFile':>6}{'片内首字节':>10}{'片内总耗时':>10}")
    print("-" * 90)
    for label, off in POINTS:
        for rnd in range(1, ROUNDS + 1):
            t_hdr, ttfb, dt, st = seek_once(off)
            time.sleep(1.2)
            info = parse_srv(new_logs())
            print(f"{label:<15}{rnd:<3}{t_hdr*1000:>7.0f}m{ttfb*1000:>7.0f}m{dt*1000:>7.0f}m  "
                  f"{info['gf']:>6}{info['first']:>10}{info['total']:>10}")
            rows.append((label, rnd, t_hdr, ttfb, dt, info))
            time.sleep(COOL)

    print("\n=== 汇总（首块中位数）===")
    print(f"{'位置':<15}{'首块中位':>9}{'最快':>9}{'最慢':>9}{'片内首字节':>13}")
    by = {}
    for label, rnd, t_hdr, ttfb, dt, info in rows:
        by.setdefault(label, {})[rnd] = (ttfb, info)
    import statistics as _st
    for label, _ in POINTS:
        d = by.get(label, {})
        vals = [v[0] for v in d.values()]
        med = _st.median(vals) if vals else 0
        f2 = d.get(max(d, default=0), (0, {}))[1].get("first", "-")
        print(f"{label:<15}{med*1000:>8.0f}m{min(vals)*1000 if vals else 0:>8.0f}m{max(vals)*1000 if vals else 0:>8.0f}m{f2:>13}")

    with open("SEEK_DIAG.md", "w", encoding="utf-8") as f:
        f.write("# seek TTFB 诊断（60MB / 3×20MB 分片，真实 Telegram）\n\n")
        f.write("| 位置 | 冷首块 | 热首块 | 热片内首字节 | 热片内总耗时 | getFile |\n|---|---|---|---|---|---|\n")
        for label, _ in POINTS:
            d = by.get(label, {})
            c1 = f"{d[1][0]*1000:.0f}ms" if 1 in d else "-"
            c2 = f"{d[2][0]*1000:.0f}ms" if 2 in d else "-"
            i2 = d.get(2, (0, {}))[1]
            f.write(f"| {label} | {c1} | {c2} | {i2.get('first','-')} | "
                    f"{i2.get('total','-')} | {i2.get('gf','-')} |\n")
    print("\n已写入 SEEK_DIAG.md")

    # 保存一个 path 供直连基线使用
    for _, _, _, _, _, info in rows:
        if info.get("path"):
            open("/tmp/seek_path.txt", "w").write(info["path"])
            print("代理 path 样例已存 /tmp/seek_path.txt")
            break


if __name__ == "__main__":
    main()
