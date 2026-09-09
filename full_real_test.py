"""全量真实场景测试（真实 Telegram bot + 真实代理，非 mock）。

目标：尽可能暴露真实缺陷。覆盖维度：
  A 目录(MKCOL/PROPFIND/DELETE)
  B 文件大小矩阵(0B/1B/1KB/1MB/20MB分片边界/45MB/100MB) 上传→校验→Range→删除
  C Range 边界与异常(首/中/尾/越界/起点超尾/非法)
  D 文件名(中文/空格/特殊字符)
  E MOVE/COPY
  F 错误处理(404/409/416)
  G 并发(PUT/GET/同文件Range)
  H 客户端中途断开后服务端健康
  I 覆盖写
每个用例都做严格断言(状态码/字节数/SHA-256)，不符即记录 FAIL 并打印实际值。
"""
import base64
import hashlib
import http.client
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("FULL_BASE", "http://127.0.0.1:10010")
USER, PASS = "totootao", "Hhangxing963."
AUTH = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
ROOT = "/fulltest"

results = []


def check(name, cond, extra=""):
    results.append((name, bool(cond), extra))
    print(("PASS " if cond else "FAIL ") + name + (f"  -> {extra}" if extra and not cond else ""))
    sys.stdout.flush()


def req(method, path, body=None, headers=None, timeout=300):
    h = {"Authorization": f"Basic {AUTH}"}
    if headers:
        h.update(headers)
    url = BASE + urllib.parse.quote(path, safe="/")
    r = urllib.request.Request(url, data=body, method=method, headers=h)
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
    parts = [p for p in path.strip("/").split("/")][:-1]
    cur = ""
    for p in parts:
        cur += "/" + p
        req("MKCOL", cur)


def put(path, data, ctype="application/octet-stream"):
    ensure_parents(path)
    return req("PUT", path, body=data, headers={"Content-Type": ctype})


def get(path, headers=None, timeout=300):
    return req("GET", path, headers=headers, timeout=timeout)


def rng(path, spec, timeout=300):
    return req("GET", path, headers={"Range": spec}, timeout=timeout)


def test_a_dirs():
    print("\n[A] 目录操作")
    d = f"{ROOT}/dirA"
    st, _, _ = req("MKCOL", d)
    check("A1 MKCOL 新建目录", st in (200, 201), f"status={st}")

    st2, _, _ = req("MKCOL", d)
    check("A2 MKCOL 已存在目录(应非2xx)", st2 not in (200, 201), f"status={st2}")

    nested = f"{ROOT}/n1/n2/n3"
    st3, _, _ = req("MKCOL", nested)
    check("A3 MKCOL 多级不存在父目录(应非2xx)", st3 not in (200, 201), f"status={st3}")

    req("MKCOL", f"{ROOT}/n1")
    req("MKCOL", f"{ROOT}/n1/n2")
    st4, _, _ = req("MKCOL", f"{ROOT}/n1/n2/n3")
    check("A4 MKCOL 父目录就绪后可建", st4 in (200, 201), f"status={st4}")

    st5, _, _ = req("PROPFIND", f"{ROOT}", headers={"Depth": "1"})
    check("A5 PROPFIND 列目录(207)", st5 == 207, f"status={st5}")

    st6, _, _ = req("DELETE", f"{ROOT}/n1/n2/n3")
    check("A6 DELETE 空目录", st6 in (200, 204), f"status={st6}")


SIZES = [
    ("0B", 0),
    ("1B", 1),
    ("1KB", 1024),
    ("1MB", 1024 * 1024),
    ("20MB", 20 * 1024 * 1024),
    ("45MB", 45 * 1024 * 1024),
    ("100MB", 100 * 1024 * 1024),
]


def test_b_sizes():
    print("\n[B] 文件大小矩阵（上传→校验→Range→删除）")
    for label, size in SIZES:
        data = os.urandom(size)
        want = sha(data)
        p = f"{ROOT}/sz/{label}.bin"
        t0 = time.time()
        st, _, _ = put(p, data)
        dt = time.time() - t0
        if st not in (200, 201, 204):
            check(f"B_{label} PUT", False, f"status={st}")
            continue
        check(f"B_{label} PUT({size}B {dt:.1f}s)", True)

        st2, h2, b2 = get(p)
        ok_sha = (sha(b2) == want)
        ok_size = (len(b2) == size)
        check(f"B_{label} GET 200+字节数+SHA",
              st2 == 200 and ok_size and ok_sha,
              f"status={st2} size={len(b2)}/{size} shaok={ok_sha}")

        st3, h3, _ = req("HEAD", p)
        cl = h3.get("Content-Length") or h3.get("content-length")
        check(f"B_{label} HEAD Content-Length",
              st3 == 200 and cl is not None and int(cl) == size,
              f"status={st3} CL={cl}/{size}")

        st4, _, b4 = req("PROPFIND", p, headers={"Depth": "0"})
        has_len = (str(size).encode() in b4) or (b"getcontentlength" in b4.lower())
        check(f"B_{label} PROPFIND 含大小属性", st4 in (200, 207) and has_len,
              f"status={st4} haslen={has_len}")

        if size >= 2 * 1024 * 1024:
            s, e = size // 3, size // 2
            st5, h5, b5 = rng(p, f"bytes={s}-{e}")
            want_seg = data[s:e + 1]
            cr = h5.get("Content-Range") or h5.get("content-range") or ""
            ok_seg = (b5 == want_seg)
            check(f"B_{label} Range 中段 206+内容+Content-Range",
                  st5 == 206 and ok_seg and f"/{size}" in cr,
                  f"status={st5} segok={ok_seg} cr={cr}")

        st6, _, _ = req("DELETE", p)
        st7, _, _ = get(p)
        check(f"B_{label} DELETE 后 GET 404",
              st6 in (200, 204) and st7 == 404,
              f"del={st6} get_after={st7}")


def test_c_range():
    print("\n[C] Range 边界与异常")
    data = os.urandom(1024 * 1024)
    n = len(data)
    p = f"{ROOT}/rng.bin"
    st, _, _ = put(p, data)
    if st not in (200, 201, 204):
        check("C_setup PUT", False, f"status={st}")
        return

    cases = [
        ("C1 首字节 bytes=0-0", "bytes=0-0", 206, data[0:1]),
        ("C2 前1KB bytes=0-1023", "bytes=0-1023", 206, data[0:1024]),
        ("C3 中段 bytes=100000-199999", "bytes=100000-199999", 206, data[100000:200000]),
        ("C4 开放尾 bytes=999000-", "bytes=999000-", 206, data[999000:]),
        ("C5 尾部后缀 bytes=-1024", "bytes=-1024", 206, data[-1024:]),
        ("C6 整段 bytes=0-", "bytes=0-", 206, data),
    ]
    for name, spec, want_st, want_body in cases:
        st2, h2, b2 = rng(p, spec)
        ok = (st2 == want_st and b2 == want_body)
        check(name, ok, f"status={st2}(期望{want_st}) len={len(b2)}(期望{len(want_body)})")

    st3, h3, b3 = rng(p, f"bytes={n - 100}-{n + 1000}")
    check("C7 终点越界(应206截断到末尾)",
          st3 == 206 and b3 == data[n - 100:],
          f"status={st3} len={len(b3)}(期望100)")

    st4, h4, b4 = rng(p, f"bytes={n + 1}-{n + 100}")
    check("C8 起点超末尾(应416)",
          st4 == 416,
          f"status={st4}(期望416) 实际返回{len(b4)}B")

    st5, h5, b5 = rng(p, "bytes=abc-def")
    check("C9 非法Range(应200忽略或400,非500)",
          st5 in (200, 400, 206),
          f"status={st5}")

    st6, _, b6 = rng(p, "bytes=100-50")
    check("C10 反转Range(应416或200,非500)",
          st6 in (200, 206, 400, 416),
          f"status={st6}")

    req("DELETE", p)


def test_d_names():
    print("\n[D] 文件名(中文/空格/特殊字符)")
    data = os.urandom(4096)
    want = sha(data)
    names = [
        "中文文件.bin",
        "with space.bin",
        "special!@#$%^&().bin",
        "a" * 100 + ".bin",
    ]
    for nm in names:
        p = f"{ROOT}/nm/{nm}"
        st, _, _ = put(p, data)
        if st not in (200, 201, 204):
            check(f"D PUT 名称[{nm[:20]}]", False, f"status={st}")
            continue
        st2, _, b2 = get(p)
        ok = (st2 == 200 and sha(b2) == want)
        check(f"D 往返 名称[{nm[:24]}]", ok,
              f"status={st2} shaok={sha(b2) == want}")
        req("DELETE", p)


def test_e_move_copy():
    print("\n[E] MOVE / COPY")
    data = os.urandom(64 * 1024)
    want = sha(data)
    src = f"{ROOT}/mv/src.bin"
    put(src, data)

    dst = f"{ROOT}/mv/renamed.bin"
    st, _, _ = req("MOVE", src, headers={"Destination": BASE + urllib.parse.quote(dst, safe="/")})
    if st in (200, 201, 204):
        st2, _, b2 = get(dst)
        check("E1 MOVE 重命名后新路径可读且内容一致",
              st2 == 200 and sha(b2) == want, f"status={st2}")
        st3, _, _ = get(src)
        check("E2 MOVE 后旧路径 404", st3 == 404, f"status={st3}")
    else:
        check("E1 MOVE 重命名", False, f"status={st}")

    req("MKCOL", f"{ROOT}/mv2")
    dst2 = f"{ROOT}/mv2/moved.bin"
    st4, _, _ = req("MOVE", dst, headers={"Destination": BASE + urllib.parse.quote(dst2, safe="/")})
    if st4 in (200, 201, 204):
        st5, _, b5 = get(dst2)
        check("E3 MOVE 跨目录后可读且一致", st5 == 200 and sha(b5) == want, f"status={st5}")
    else:
        check("E3 MOVE 跨目录", False, f"status={st4}")

    cp = f"{ROOT}/mv2/copied.bin"
    st6, _, _ = req("COPY", dst2, headers={"Destination": BASE + urllib.parse.quote(cp, safe="/")})
    if st6 in (200, 201, 204):
        st7, _, b7 = get(cp)
        check("E4 COPY 目标可读且一致", st7 == 200 and sha(b7) == want, f"status={st7}")
        st8, _, b8 = get(dst2)
        check("E5 COPY 后源仍存在", st8 == 200 and sha(b8) == want, f"status={st8}")
    else:
        check("E4 COPY", False, f"status={st6} (COPY 可能未实现)")


def test_f_errors():
    print("\n[F] 错误处理")
    st, _, _ = get(f"{ROOT}/no_such_file_xyz.bin")
    check("F1 GET 不存在文件 → 404", st == 404, f"status={st}")

    st2, _, _ = req("DELETE", f"{ROOT}/no_such_file_xyz.bin")
    check("F2 DELETE 不存在 → 404", st2 == 404, f"status={st2}")

    st3, _, _ = req("PROPFIND", f"{ROOT}/no_such_dir_xyz", headers={"Depth": "1"})
    check("F3 PROPFIND 不存在目录 → 404", st3 == 404, f"status={st3}")

    st4, _, _ = req("PUT", f"{ROOT}/no_parent_xyz/f.bin", body=b"x",
                    headers={"Content-Type": "application/octet-stream"})
    check("F4 PUT 无父目录 → 非2xx", st4 not in (200, 201, 204), f"status={st4}")

    bad = base64.b64encode(b"totootao:wrongpass").decode()
    try:
        r = urllib.request.Request(BASE + "/", headers={"Authorization": f"Basic {bad}"})
        with urllib.request.urlopen(r, timeout=20) as resp:
            st5 = resp.status
    except urllib.error.HTTPError as e:
        st5 = e.code
    except Exception as e:
        st5 = None
    check("F5 错误密码 → 401", st5 == 401, f"status={st5}")


def test_g_concurrent():
    print("\n[G] 并发")
    files = {}
    for i in range(3):
        files[f"{ROOT}/conc/c{i}.bin"] = os.urandom(3 * 1024 * 1024)

    errs = {}

    def one_put(p, d):
        try:
            st, _, _ = put(p, d)
            if st not in (200, 201, 204):
                errs[f"put{p}"] = st
        except Exception as e:
            errs[f"put{p}"] = f"{type(e).__name__}: {e}"

    ts = [threading.Thread(target=one_put, args=(p, d)) for p, d in files.items()]
    [t.start() for t in ts]
    [t.join() for t in ts]
    check("G1 3 文件并发 PUT", not errs, f"errors={errs}")

    res = {}

    def one_get(p, d):
        try:
            st, _, b = get(p)
            res[p] = (st, sha(b) == sha(d), len(b))
        except Exception as e:
            res[p] = (None, False, f"{type(e).__name__}: {e}")

    ts = [threading.Thread(target=one_get, args=(p, d)) for p, d in files.items()]
    [t.start() for t in ts]
    [t.join() for t in ts]
    allok = all(v[0] == 200 and v[1] for v in res.values())
    check("G2 3 文件并发 GET 全部 SHA 一致", allok, f"res={res}")

    p0 = list(files.keys())[0]
    d0 = files[p0]
    n = len(d0)
    seg_errs = []

    def one_rng(i):
        s = i * (n // 6)
        e = s + (n // 6) - 1
        st, _, b = rng(p0, f"bytes={s}-{e}")
        if st != 206 or b != d0[s:e + 1]:
            seg_errs.append((i, st, len(b)))

    ts = [threading.Thread(target=one_rng, args=(i,)) for i in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    check("G3 同文件 6 段并发 Range 全部正确", not seg_errs, f"errs={seg_errs}")

    for p in files:
        req("DELETE", p)


def test_h_abort():
    print("\n[H] 客户端中途断开后服务端健康")
    data = os.urandom(20 * 1024 * 1024)
    p = f"{ROOT}/abort.bin"
    put(p, data)

    host, port = BASE.replace("http://", "").split(":")
    closed_ok = False
    try:
        s = socket.create_connection((host, int(port)), timeout=30)
        s.sendall(f"GET {urllib.parse.quote(p, safe='/')} HTTP/1.1\r\n"
                  f"Host: {host}:{port}\r\n"
                  f"Authorization: Basic {AUTH}\r\n\r\n".encode())
        time.sleep(0.4)
        s.close()
        closed_ok = True
    except Exception as e:
        check("H1 建立连接并中途关闭", False, f"{type(e).__name__}: {e}")

    if closed_ok:
        time.sleep(0.5)
        st, _, b = get(p, timeout=300)
        check("H2 客户端断开后再次完整 GET 仍 200+SHA 一致",
              st == 200 and sha(b) == sha(data),
              f"status={st} len={len(b)}")
        st2, _, _ = req("PROPFIND", ROOT, headers={"Depth": "1"})
        check("H3 断开后服务端仍响应 PROPFIND(207)", st2 == 207, f"status={st2}")

    req("DELETE", p)


def test_i_overwrite():
    print("\n[I] 覆盖写")
    p = f"{ROOT}/ow.bin"
    d1 = os.urandom(1024 * 1024)
    put(p, d1)
    st2, _, b2 = get(p)
    check("I1 首次写入可读", st2 == 200 and sha(b2) == sha(d1), f"status={st2}")

    d2 = os.urandom(2 * 1024 * 1024)
    put(p, d2)
    st4, _, b4 = get(p)
    check("I2 覆盖写后读到新内容(非旧内容)",
          st4 == 200 and sha(b4) == sha(d2) and sha(b4) != sha(d1),
          f"status={st4} new={sha(b4) == sha(d2)} old={sha(b4) == sha(d1)}")

    st5, h5, _ = req("HEAD", p)
    cl = h5.get("Content-Length") or h5.get("content-length")
    check("I3 覆盖写后 HEAD 长度更新", cl and int(cl) == len(d2), f"CL={cl}/{len(d2)}")

    req("DELETE", p)


def main():
    print("=" * 60)
    print(f"全量真实场景测试  BASE={BASE}")
    print("=" * 60)
    req("MKCOL", ROOT)
    test_a_dirs()
    test_b_sizes()
    test_c_range()
    test_d_names()
    test_e_move_copy()
    test_f_errors()
    test_g_concurrent()
    test_h_abort()
    test_i_overwrite()

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
