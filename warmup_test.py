"""file_path 预热 A/B 实测（真实 Telegram + 自建代理）。

三种路径对比「首片 TTFB」：
  S1 冷启动直接 GET          —— 基线，getFile 在关键路径上
  S2 PROPFIND 列目录 → GET   —— 列目录时后台预热首片
  S3 HEAD 探测 → GET         —— 探测时预热全部片

每轮都重启服务端以清空 file_path 内存缓存，保证可比。
"""
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request
import base64

BASE = os.environ.get("WARM_BASE", "http://127.0.0.1:10011")
USER, PASS = "totootao", "Hhangxing963."
ENV = dict(os.environ)
ENV.update({
    "TG_API_BASE": "https://tg.totootao.top/tg",
    "TG_PROXY_TOKEN": "96b833fb8a57c88bd3ce4bcf879941aa3679160c7d20258e923b5e937c2d8946",
    "TG_BOT_POOLS": '[{"token":"6052003609:AAHUNBLTtqCEpgMxgMvs8gFXtgdYph8Zj5I","chatId":"-1001549117195"},{"token":"8981700038:AAGDAC819x2_Ozm-Kg8m9GIm0RIP5RJbewI","chatId":"-1001929321614"},{"token":"8912106224:AAEVjCawYULB6agUkBkRCgfF61ZBAQC71MM","chatId":"-1001945524123"},{"token":"8813237599:AAHEDQGxZdHZTWVpSI5TDH_cGuJoqFPNZpk","chatId":"-1001961363514"},{"token":"8998163731:AAE2k2xoqMmld5d1Pf8ko9E8pPwILsZCW8M","chatId":"-1003915360653"}]',
    "DAV_USER": USER, "DAV_PASSWORD": PASS,
    "HOST": "127.0.0.1", "PORT": "10011",
    "DB_PATH": "/home/docker/data/tg-webdav/telegram_webdav.db",
})
LOG = "/tmp/warm_srv.log"
AUTH = base64.b64encode(f"{USER}:{PASS}".encode()).decode()


def req(method, path, headers=None, timeout=120, body=None):
    h = {"Authorization": f"Basic {AUTH}"}
    h.update(headers or {})
    r = urllib.request.Request(BASE + path, method=method, headers=h, data=body)
    t0 = time.time()
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        body = resp.read()
    return resp.status, time.time() - t0, len(body)


def restart():
    subprocess.run("fuser -k 10011/tcp", shell=True, capture_output=True)
    time.sleep(1.5)
    open(LOG, "w").close()
    p = subprocess.Popen([sys.executable, "server.py"], cwd="/workspace/telegram-webdav",
                         env=ENV, stdout=open(LOG, "a"), stderr=subprocess.STDOUT)
    for _ in range(40):
        time.sleep(0.5)
        try:
            req("PROPFIND", "/", {"Depth": "0"}, timeout=5)
            return True
        except Exception:
            pass
    return False


def last_ttfb(marker_path=None):
    """从服务端日志取最近一次下载的 TTFB（`GET 分片[0](首) ... TTFB=x s(自请求起)`）。

    服务端自测比客户端计时更准：它排除了客户端自身的 auth 往返与本地开销。
    """
    out = None
    try:
        txt = open(LOG, errors="replace").read()
    except FileNotFoundError:
        return None
    for line in txt.splitlines():
        if "GET 分片" in line and "TTFB=" in line:
            m = re.search(r"TTFB=([\d.]+)s", line)
            if m:
                out = float(m.group(1))
    return out


def run_case(name, steps, target):
    ok = restart()
    if not ok:
        print(f"  {name}: 服务启动失败")
        return None
    for label, fn in steps:
        fn()
    st, dt, n = req("GET", target)
    ttfb = last_ttfb(target)
    hit = "命中" if subprocess.run(
        f"grep -c 'getFile 命中缓存' {LOG}", shell=True, capture_output=True
    ).stdout.strip() != b"0" else "未命中"
    print(f"  {name:<34} status={st} 服务端TTFB={ttfb if ttfb is not None else '-'}s "
          f"客户端总耗时={dt:.3f}s 缓存{hit}")
    return ttfb


def run_case(name, steps, target, repeat=3):
    """同一场景跑 repeat 轮（每轮重启清缓存），取中位数——代理 RTT 有波动，单点不可信。"""
    ttfbs, dts, misses = [], [], 0
    for _ in range(repeat):
        if not restart():
            print(f"  {name}: 服务启动失败")
            return None
        for label, fn in steps:
            fn()
        st, dt, n = req("GET", target)
        ttfb = last_ttfb()
        # GET 期间若出现 "getFile 请求"（非预取路径）说明首片没命中缓存
        miss = subprocess.run(
            f"grep -c 'getFile 请求' {LOG}", shell=True, capture_output=True
        ).stdout.strip()
        try:
            miss = int(miss)
        except ValueError:
            miss = 0
        misses += 1 if miss else 0
        if ttfb is not None:
            ttfbs.append(ttfb)
        dts.append(dt)
        if st != 200 or n == 0:
            print(f"  {name}: 异常 status={st} bytes={n}")
            return None
    med = lambda xs: sorted(xs)[len(xs) // 2]
    m_ttfb, m_dt = med(ttfbs), med(dts)
    print(f"  {name:<34} TTFB中位={m_ttfb:.3f}s (各轮 {['%.3f' % x for x in ttfbs]}) "
          f"下载中位={m_dt:.3f}s 首片未命中缓存={misses}/{repeat}轮")
    return m_ttfb


def ensure_big(path="/warmtest/big.bin", mb=40):
    """确保存在多分片大文件（只上传一次，后续复用同一 file_id）。"""
    try:
        st, _, n = req("HEAD", path)
        if st == 200:
            print(f"  复用已有大文件 {path}\n")
            return path
    except Exception:
        pass
    print(f"  首次上传 {mb}MB 大文件（真实 sendDocument，分片到多 bot）...")
    body = os.urandom(mb * 1024 * 1024)
    t0 = time.time()
    st, _, _ = req("PUT", path, {"Content-Type": "application/octet-stream"},
                   timeout=900, body=body)
    dt = time.time() - t0
    if st not in (200, 201, 204):
        print(f"  上传失败 status={st}")
        return None
    print(f"  上传完成 status={st} 耗时={dt:.2f}s ({mb/dt:.2f} MB/s)\n")
    return path


def suite(title, target):
    print(f"=== {title} ===\n")
    t1 = run_case("S1 冷启动直接 GET（基线）", [], target)
    t2 = run_case("S2 PROPFIND 列目录 → 等2s → GET", [
        ("PROPFIND", lambda: req("PROPFIND", "/warmtest", {"Depth": "1"})),
        ("等2s", lambda: time.sleep(2)),
    ], target)
    t3 = run_case("S3 HEAD 探测 → 等1s → GET", [
        ("HEAD", lambda: req("HEAD", target)),
        ("等1s", lambda: time.sleep(1)),
    ], target)
    print("  --- 汇总 ---")
    if t1:
        for nm, v in (("S1 无预热", t1), ("S2 PROPFIND预热", t2), ("S3 HEAD预热", t3)):
            if v is not None:
                print(f"    {nm:<18} TTFB={v:.3f}s  相对基线 {((t1 - v) / t1 * 100):+.1f}%")
    print()
    return (t1, t2, t3)


print("=== file_path 预热 A/B（真实环境，每轮重启清缓存）===\n")
if os.environ.get("SKIP_SMALL") != "1":
    suite("小文件 2KB（单片）", "/warmtest/w1.bin")
big = ensure_big()
if big:
    suite("大文件 40MB（多分片，播放场景）", big)
