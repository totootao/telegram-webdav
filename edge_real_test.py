"""边界与压力专项测试（真实环境）：专挑最易出错的地方打。

重点：
  R  Range 跨分片边界（off-by-one 高发区）+ 跨多分片 + 后缀 Range + 断点续传拼接
  P  深层路径 / 超长文件名 / 大目录(100项) PROPFIND
  S  高并发(12路) GET 与 Range
  SEC 路径遍历安全（../ 与 URL 编码）
  M  OPTIONS 能力探测
"""
import base64
import hashlib
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("EDGE_BASE", "http://127.0.0.1:10010")
USER, PASS = "totootao", "Hhangxing963."
AUTH = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
ROOT = "/edgetest"

results = []


def check(name, cond, extra=""):
    results.append((name, bool(cond), extra))
    print(("PASS " if cond else "FAIL ") + name + (f"  -> {extra}" if extra and not cond else ""))
    sys.stdout.flush()


def req(method, path, body=None, headers=None, timeout=300):
    h = {"Authorization": f"Basic {AUTH}"}
    if headers:
        h.update(headers)
    r = urllib.request.Request(BASE + urllib.parse.quote(path, safe="/"),
                               data=body, method=method, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        return None, {}, f"{type(e).__name__}: {e}".encode()


def sha(b):
    return hashlib.sha256(b).hexdigest()


def ensure_parents(path):
    parts = path.strip("/").split("/")[:-1]
    cur = ""
    for p in parts:
        cur += "/" + p
        req("MKCOL", cur)


def put(path, data):
    ensure_parents(path)
    return req("PUT", path, body=data, headers={"Content-Type": "application/octet-stream"})


def rng(path, spec):
    return req("GET", path, headers={"Range": spec})


def get(path):
    return req("GET", path)


def test_r_boundaries():
    """45MB 文件，分片 20MB → 片0:0-20971519 片1:20971520-41943039 片2:41943040-..."""
    print("\n[R] Range 跨分片边界 / 跨多分片 / 断点续传")
    size = 45 * 1024 * 1024
    data = os.urandom(size)
    CS = 20 * 1024 * 1024  # 分片大小
    p = f"{ROOT}/edge.bin"
    st, _, _ = put(p, data)
    if st not in (200, 201, 204):
        check("R_setup PUT 45MB", False, f"status={st}")
        return
    check("R_setup PUT 45MB", True)

    b0, b1 = CS - 1, CS          # 20971519 / 20971520  片0|片1 边界
    b2, b3 = 2 * CS - 1, 2 * CS  # 41943039 / 41943040  片1|片2 边界

    cases = [
        ("R1 边界前1字节", f"bytes={b0 - 1}-{b0 - 1}", data[b0 - 1:b0]),
        ("R2 片0最后1字节", f"bytes={b0}-{b0}", data[b0:b0 + 1]),
        ("R3 跨片0→片1(2B)", f"bytes={b0}-{b1}", data[b0:b1 + 1]),
        ("R4 跨边界±10", f"bytes={b0 - 10}-{b1 + 10}", data[b0 - 10:b1 + 11]),
        ("R5 片1首字节", f"bytes={b1}-{b1}", data[b1:b1 + 1]),
        ("R6 跨片1→片2(2B)", f"bytes={b2}-{b3}", data[b2:b3 + 1]),
        ("R7 跨边界±10(片1/2)", f"bytes={b2 - 10}-{b3 + 10}", data[b2 - 10:b3 + 11]),
        ("R8 跨整个片1", f"bytes={b1}-{b2}", data[b1:b2 + 1]),
        ("R9 跨3片(10MB-40MB)", f"bytes={10 * 1024 * 1024}-{40 * 1024 * 1024}",
         data[10 * 1024 * 1024:40 * 1024 * 1024 + 1]),
        ("R10 最后1字节", f"bytes={size - 1}-{size - 1}", data[size - 1:]),
        ("R11 后缀 bytes=-1", "bytes=-1", data[-1:]),
        ("R12 后缀跨片 bytes=-20971521", "bytes=-20971521", data[-20971521:]),
        ("R13 整文件", f"bytes=0-{size - 1}", data),
        ("R14 单点中段", f"bytes={size // 2}-{size // 2}", data[size // 2:size // 2 + 1]),
    ]
    for name, spec, want in cases:
        st2, h2, b2 = rng(p, spec)
        cr = h2.get("Content-Range") or h2.get("content-range") or ""
        ok = (st2 == 206 and b2 == want and cr.endswith(f"/{size}"))
        check(name, ok,
              f"status={st2} len={len(b2)}/{len(want)} shaok={sha(b2) == sha(want)} cr={cr}")

    # 断点续传：分 5 段顺序下载拼成完整文件
    pieces = []
    ok_all = True
    seg = size // 5
    for i in range(5):
        s = i * seg
        e = (size - 1) if i == 4 else (s + seg - 1)
        st3, _, b3 = rng(p, f"bytes={s}-{e}")
        if st3 != 206 or b3 != data[s:e + 1]:
            ok_all = False
            break
        pieces.append(b3)
    joined = b"".join(pieces)
    check("R15 断点续传(5段拼接)SHA 一致",
          ok_all and sha(joined) == sha(data),
          f"拼得={len(joined)}/{size} shaok={sha(joined) == sha(data)}")

    req("DELETE", p)


def test_p_paths():
    print("\n[P] 深层路径 / 长文件名 / 大目录")
    deep = f"{ROOT}/a/b/c/d/e/f.bin"
    data = os.urandom(4096)
    st, _, _ = put(deep, data)
    if st in (200, 201, 204):
        st2, _, b2 = get(deep)
        check("P1 5层深层路径往返", st2 == 200 and sha(b2) == sha(data), f"status={st2}")
    else:
        check("P1 5层深层路径 PUT", False, f"status={st}")

    long_name = "L" * 200 + ".bin"
    lp = f"{ROOT}/long/{long_name}"
    st3, _, _ = put(lp, data)
    if st3 in (200, 201, 204):
        st4, _, b4 = get(lp)
        check("P2 200字符文件名往返", st4 == 200 and sha(b4) == sha(data), f"status={st4}")
    else:
        check("P2 200字符文件名 PUT", False, f"status={st3}")

    # 大目录：100 个文件
    n = 100
    for i in range(n):
        put(f"{ROOT}/big_dir/f{i:03d}.bin", os.urandom(64))
    st5, _, b5 = req("PROPFIND", f"{ROOT}/big_dir", headers={"Depth": "1"})
    listed = b5.count(b"f0")  # 粗略计数
    check("P3 大目录 PROPFIND(100项) 返回207", st5 == 207, f"status={st5}")
    check("P4 大目录列表项数合理(>=50)", listed >= 50, f"粗略计数={listed}")


def test_s_concurrent():
    print("\n[S] 高并发（12 路）")
    data = os.urandom(8 * 1024 * 1024)
    p = f"{ROOT}/conc/hot.bin"
    put(p, data)
    n = len(data)
    errs = []

    def one(i):
        s = (i * (n // 12))
        e = s + 1024 * 1024 - 1
        if e >= n:
            return
        st, _, b = rng(p, f"bytes={s}-{e}")
        if st != 206 or b != data[s:e + 1]:
            errs.append((i, st, len(b)))

    ts = [threading.Thread(target=one, args=(i,)) for i in range(12)]
    t0 = time.time()
    [t.start() for t in ts]
    [t.join() for t in ts]
    dt = time.time() - t0
    check(f"S1 12 路并发 Range 全部正确({dt:.1f}s)", not errs, f"errs={errs[:5]}")

    # 12 路并发全量 GET
    g_errs = []

    def one_get(i):
        st, _, b = get(p)
        if st != 200 or sha(b) != sha(data):
            g_errs.append((i, st, len(b)))

    ts = [threading.Thread(target=one_get, args=(i,)) for i in range(12)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    check("S2 12 路并发全量 GET 全部 SHA 一致", not g_errs, f"errs={g_errs[:5]}")

    req("DELETE", p)


def test_sec_traversal():
    print("\n[SEC] 路径遍历安全")
    st, _, b = req("GET", "/../../../etc/passwd")
    leaked = b"root:" in b
    check("SEC1 路径遍历 ../../../etc/passwd 不泄露",
          not leaked and st in (404, 403, 400), f"status={st} leaked={leaked}")

    st2, _, b2 = req("GET", "/%2e%2e%2f%2e%2e%2fetc%2fpasswd")
    leaked2 = b"root:" in b2
    check("SEC2 URL编码路径遍历不泄露",
          not leaked2 and st2 in (404, 403, 400), f"status={st2} leaked={leaked2}")


def test_m_options():
    print("\n[M] OPTIONS / 能力探测")
    st, h, b = req("OPTIONS", "/")
    dav = h.get("DAV") or h.get("dav") or ""
    allow = h.get("Allow") or h.get("allow") or ""
    check("M1 OPTIONS 返回 200 且声明 DAV 能力",
          st == 200 and dav != "",
          f"status={st} DAV={dav} Allow={allow}")
    if allow:
        need = [m for m in ("GET", "PUT", "DELETE", "PROPFIND", "MKCOL", "MOVE")]
        missing = [m for m in need if m not in allow]
        check("M2 Allow 含核心方法", not missing, f"缺失={missing}")


def main():
    print("=" * 60)
    print(f"边界与压力专项测试  BASE={BASE}")
    print("=" * 60)
    req("MKCOL", ROOT)
    test_r_boundaries()
    test_p_paths()
    test_s_concurrent()
    test_sec_traversal()
    test_m_options()

    ok = sum(1 for _, c, _ in results if c)
    bad = [r for r in results if not r[1]]
    print("\n" + "=" * 60)
    print(f"结果: {ok} 通过 / {len(bad)} 失败  (共 {len(results)})")
    if bad:
        print("\n失败明细:")
        for name, _, extra in bad:
            print(f"  - {name}   {extra}")
    print("=" * 60)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
