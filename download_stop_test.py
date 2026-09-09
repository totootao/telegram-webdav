#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""下载过程随机停止专项测试。

背景：telegram-webdav 历史上「下载随机停止」其实由 5 个独立 bug 共同引发：
  P0: 已向客户端写出字节后切代理候选 → 重复下发字节（污染响应/破完整性）
  P1: 连接池无 TTL/容量/归还校验 → ResponseNotReady / RemoteDisconnected 连锁自伤
  P2: 代理少发字节（CL 虚高）→ resp.read 安静返回 b"" → 静默截断但日志写"完成"
  P3: _do_get 的 timeout 形参失效 + 黑洞场景无逃生路径 → 18 分钟挂起
  P4: 流式写响应体受 idle_timeout 限制 → 客户端慢就被误判为「客户端断开」

本测试用 fake_telegram 的故障注入钩子（FAIL_FLAGS）模拟「真生产代理」行为，
端到端跑过 webdav 的下载路径，断言所有症状都被修掉。

用法：
    python3.11 download_stop_test.py

注意：本测试用 fake_telegram 起独立服务，不依赖外网。
"""
import base64
import hashlib
import http.client
import inspect
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ============== 测试环境准备 ==============
def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


FAKE_PORT = _free_port()
DAV_PORT = _free_port()
DB_PATH = os.path.join("/tmp", f"tgwebdav_dlstop_{os.getpid()}.db")

os.environ["TG_API_BASE"] = f"http://127.0.0.1:{FAKE_PORT}"
os.environ["TG_BOT_TOKEN"] = "FAKE_TOKEN"
os.environ["TG_CHAT_ID"] = "-100FAKE"
os.environ["CHUNK_SIZE_MB"] = "5"
os.environ["DB_PATH"] = DB_PATH
os.environ["PORT"] = str(DAV_PORT)
os.environ["HOST"] = "127.0.0.1"
os.environ["DAV_USER"] = "dlstop"
os.environ["DAV_PASSWORD"] = "p@ss"
os.environ["TG_WEBHOOK_SECRET"] = "sekret"
os.environ["DAV_IDLE_TIMEOUT"] = "2"  # 短一点，跑得快

import fake_telegram as ftg
from server import make_server

ftg.start_fake(FAKE_PORT)
srv = make_server()
_srv_thread = threading.Thread(target=srv.serve_forever, daemon=True)
_srv_thread.start()
time.sleep(0.5)

BASE = f"http://127.0.0.1:{DAV_PORT}"
AUTH = "Basic " + base64.b64encode(b"dlstop:p@ss").decode()


# ============== 测试工具 ==============
def req(method, path, body=None, headers=None, timeout=60):
    """走 urllib 的标准请求，自动 try/except。返回 (status, headers, body_bytes)。

    服务端主动切连接时 urllib 会抛 IncompleteRead；这里把"已收的部分字节"作为 body 返回，
    便于测试断言"未静默成功"。
    """
    h = {"Authorization": AUTH, "Connection": "close"}
    if headers:
        h.update(headers)
    r = urllib.request.Request(BASE + path, data=body, method=method, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except http.client.IncompleteRead as e:
        # 服务端提前 FIN = 修复后的正常行为；返回"已收部分 + 标识非全"
        # 用 status=599 作为内部测试用的"异常但有 body"约定值（HTTP 标准无此码）
        return 599, {}, e.partial


def sha(b):
    return hashlib.sha256(b).hexdigest()


def ensure_parents():
    """建好父目录（保证 PUT 不被 409 拒绝）。"""
    for p in ("/", "/dlstop", "/dlstop/sub"):
        req("MKCOL", p)


# 结果收集
results = []
def check(name, cond, extra=""):
    results.append((name, bool(cond), extra))
    print(("PASS " if cond else "FAIL ") + name + (("  -> " + extra) if extra and not cond else ""))


# ============== A 组：池污染自愈（服务端主动 FIN 后复用） ==============
def test_a_pool_recovery():
    print("\n[A] 池污染自愈（服务端主动 FIN 后复用检测）")
    ensure_parents()
    payload = os.urandom(5 * 1024 * 1024)
    payload_sha = sha(payload)
    # 让 fake_telegram 在所有 /file/ 响应里都强制 Connection: close（模拟代理清连接）
    ftg.reset_fail_flags()
    ftg.FAIL_FLAGS["send_connection_close_after"] = 1
    try:
        st, _, _ = req("PUT", "/dlstop/sub/pool_a.bin", body=payload,
                       headers={"Content-Type": "application/octet-stream"})
        if st not in (200, 201):
            check("A_setup.put_ok", False, f"PUT 失败: {st}")
            return
        # GET 全量：会走多次 /file/ 请求；如果池里的连接被服务端 FIN 标记，
        # 修复前会在第 2 次请求时 ResponseNotReady / BadStatusLine / 收到截断数据。
        t0 = time.time()
        st, h, b = req("GET", "/dlstop/sub/pool_a.bin", timeout=30)
        dt = time.time() - t0
        check("A1 池污染后 GET 仍能拿到完整数据",
              st == 200 and b == payload,
              f"status={st} 大小={len(b)}/{len(payload)} 耗时={dt:.2f}s")
        check("A2 GET 数据 SHA 与上传一致",
              sha(b) == payload_sha,
              f"got={sha(b)[:12]}... exp={payload_sha[:12]}...")
    finally:
        ftg.reset_fail_flags()


# ============== B 组：代理少发字节 / CL 虚高 ==============
def test_b_cl_too_high():
    """CL 虚高场景：fake_telegram 把 CL 声称 4MB，但只发 2MB 就断。

    修复前（真 bug）：
      iter_chunk 看到 b"" 就静默结束，_stream_chunk 返回 sent=2MB，_serve_file 看到
      sent==2MB==Content-Length→写「GET 完成」，但 keep-alive 上其实还挂着后续请求的
      字节流污染。客户端 urllib 会撞 IncompleteRead。

    修复后（修好）：
      iter_chunk 主动抛 TGError（"代理少发字节"），_serve_file 强制关连接 + close_connection=True。
      客户端会立即抛异常，而非拿到"看似 200 成功但实际不可信"的响应。

    本测试断言：客户端一定不会拿到「看起来正常收尾、读出来的 body 仍是真数据」的 200。
    （注意：这断言的是"不会静默骗客户端"，而不是"必须抛异常"——因为含 CL 的客户端
    可能撞 IncompleteRead 或在断连后 urllib 重试"成功"假象，这都算修复后正常。）
    """
    print("\n[B] CL 虚高（fake 声称 4MB 实际发 2MB）")
    ensure_parents()
    payload = os.urandom(2 * 1024 * 1024)
    ftg.reset_fail_flags()
    # fake_telegram 第 1 次 /file/ 响应：声明 CL=4MB，实际 seg 还是 2MB
    ftg.FAIL_FLAGS["wrong_content_length"] = (1, 4 * 1024 * 1024)
    try:
        st, _, _ = req("PUT", "/dlstop/sub/cl_b.bin", body=payload,
                       headers={"Content-Type": "application/octet-stream"})
        if st not in (200, 201):
            check("B_setup.put_ok", False, f"PUT 失败: {st}")
            return
        # 拿到的 body 必须不是「上传时的数据」——因为声明 4MB 实际只发 2MB，客户端要么
        # 撞 IncompleteRead，要么拿到 2MB 但保持 keep-alive 失败。绝不能"假装成功收完 4MB"。
        body = b""
        st = None
        try:
            st, h, body = req("GET", "/dlstop/sub/cl_b.bin", timeout=15)
        except (urllib.error.HTTPError, urllib.error.URLError, ConnectionError,
                http.client.IncompleteRead, OSError):
            pass  # 异常本身就算修复后行为
        # 关键断言：拿到的是「端点数据」(payload) 且 status==200（看起来 200 成功收齐）= bug
        bug_path = (st == 200 and body == payload)
        check("B1 CL 虚高：未发生'静默成功截断'（旧 bug 路径）",
              not bug_path,
              f"status={st} body_len={len(body)}={len(payload)}? payload_match={body == payload}")
    finally:
        ftg.reset_fail_flags()


def test_b_truncate_at():
    """截断：发一半就断流（无 CL 虚高，但 body 提前 EOF）。"""
    print("\n[B'] 截断（tg 提前 FIN，CL 准确）")
    ensure_parents()
    payload = os.urandom(2 * 1024 * 1024)
    ftg.reset_fail_flags()
    ftg.FAIL_FLAGS["truncate_at"] = (1, 1024 * 1024)
    try:
        st, _, _ = req("PUT", "/dlstop/sub/trunc_b.bin", body=payload,
                       headers={"Content-Type": "application/octet-stream"})
        if st not in (200, 201):
            check("B'_setup.put_ok", False, f"PUT 失败: {st}")
            return
        got_exception = False
        st = None
        b = b""
        try:
            st, h, b = req("GET", "/dlstop/sub/trunc_b.bin", timeout=30)
        except (urllib.error.HTTPError, urllib.error.URLError, ConnectionError, OSError):
            got_exception = True
        check("B'1 截断 → 客户端异常或非 200（不是静默截断）",
              got_exception or (st != 200) or (b != payload),
              f"exception={got_exception} status={st} len={len(b)}/{len(payload)}")
    finally:
        ftg.reset_fail_flags()


# ============== C 组：已写字节后切代理 ==============
def test_c_no_double_send():
    """已向客户端写出字节后不再切代理候选（避免重复字节污染）。

    修复前：会从候选 0 切到候选 1，已 yield 的字节被重复下发 → Content-Length 帧错位
    修复后：已 yield 字节后任何异常都不切候选，直接抛错让客户端收到异常

    用 15MB 的 payload + 5MB chunk_size → 3 片。fake_telegram truncate 第 2 次 /file/
    请求（即第 2 片下载），只发 512KB 而非 5MB。
    """
    print("\n[C] 已写字节后不再切代理候选（避免重复字节污染）")
    ensure_parents()
    payload = os.urandom(15 * 1024 * 1024)  # 15MB → 5/5/5 三片
    ftg.reset_fail_flags()
    ftg.FAIL_FLAGS["truncate_at"] = (2, 512 * 1024)
    try:
        st, _, _ = req("PUT", "/dlstop/sub/double_c.bin", body=payload,
                       headers={"Content-Type": "application/octet-stream"})
        if st not in (200, 201):
            check("C_setup.put_ok", False, f"PUT 失败: {st}")
            return
        # 跑 5 次：只要有一次客户端"成功收到完整 body" 就算异常（修好后必失败）
        success_count = 0
        failure_count = 0
        for _ in range(5):
            try:
                st, _, b = req("GET", "/dlstop/sub/double_c.bin", timeout=30)
                if st == 200 and b == payload:
                    success_count += 1
                else:
                    failure_count += 1
            except Exception:
                failure_count += 1
        # 修复后行为：truncate_at 让第 2 片只能收到 512KB，total != payload，客户端必异常
        check("C1 已写字节后切候选：客户端未拿到'完整但重复'的成功响应",
              success_count == 0,
              f"成功={success_count} 失败={failure_count} （修复后所有 run 都应失败）")
    finally:
        ftg.reset_fail_flags()


# ============== D 组：客户端 abort 后 keep-alive 仍干净 ==============
def test_d_keepalive_after_abort():
    """客户端收到一半主动断开，再发新请求能正常 200。

    修复前：客户端断开 → _stream_chunk 抛 _ClientGone → 生成器被 close → 在 yield 处
            抛 GeneratorExit → finally 在静默态下用 alive=True 归还半残连接 → 下次请求
            ResponseNotReady → 200 被破坏。
    修复后：alive 判定加上 resp.isclosed() 校验 + finally 不再无条件 Trust 上游 alive=False，
            半残连接被关，下一次请求建立新连接拿数据。
    """
    print("\n[D] 客户端 abort 后 keep-alive 干净")
    ensure_parents()
    payload = os.urandom(3 * 1024 * 1024)
    ftg.reset_fail_flags()
    try:
        st, _, _ = req("PUT", "/dlstop/sub/abort_d.bin", body=payload,
                       headers={"Content-Type": "application/octet-stream"})
        if st not in (200, 201):
            check("D_setup.put_ok", False, f"PUT 失败: {st}")
            return

        # 自定义 GET：发请求、读到约一半时强行关读端（模拟客户端 abort）
        import http.client as _hc
        c = _hc.HTTPConnection("127.0.0.1", DAV_PORT, timeout=10)
        try:
            c.request("GET", "/dlstop/sub/abort_d.bin",
                      headers={"Authorization": AUTH, "Connection": "keep-alive"})
            resp = c.getresponse()
            got = 0
            try:
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    got += len(chunk)
                    if got >= 1024 * 1024:
                        break
            except Exception:
                pass
        finally:
            try:
                c.close()
            except Exception:
                pass

        time.sleep(0.3)

        st, h, b = req("GET", "/dlstop/sub/abort_d.bin", timeout=30)
        check("D1 abort 后下一次完整 GET 仍然 200 + 内容一致",
              st == 200 and b == payload,
              f"status={st} 大小={len(b)}/{len(payload)} 前次只读={got}")
    finally:
        ftg.reset_fail_flags()


# ============== E 组：黑洞代理超时 ==============
def test_e_blackhole_timeout():
    """黑洞代理：accept 但不回包。修复前会挂 18 分钟；修复后应超时。

    fake_telegram 在 black_hole flag 下：回响应头后 sleep N 秒才关，模拟「黑洞」。
    """
    print("\n[E] 黑洞代理：在合理超时内报错，不挂死 18 分钟")
    ensure_parents()
    payload = os.urandom(3 * 1024 * 1024)
    ftg.reset_fail_flags()
    ftg.FAIL_FLAGS["black_hole"] = (1, 1.5)
    try:
        st, _, _ = req("PUT", "/dlstop/sub/bhole_e.bin", body=payload,
                       headers={"Content-Type": "application/octet-stream"})
        if st not in (200, 201):
            check("E_setup.put_ok", False, f"PUT 失败: {st}")
            return
        t0 = time.time()
        got = None
        try:
            st, h, b = req("GET", "/dlstop/sub/bhole_e.bin", timeout=20)
            got = (st, b)
        except Exception as e:
            got = e
        dt = time.time() - t0
        # 修复前最坏 18 分钟，修复后应在 20s 内返回
        check("E1 黑洞场景在合理时间内（<10s）出错", dt < 10.0, f"耗时={dt:.2f}s")
        check("E2 黑洞场景未返回'完整截断'成功响应",
              not (isinstance(got, tuple) and got[0] == 200 and len(got[1]) == len(payload)),
              f"结果类型={type(got).__name__ if not isinstance(got, tuple) else 'tuple'}")
    finally:
        ftg.reset_fail_flags()


# ============== F 组：连接池 TTL/容量/归还校验 ==============
def test_f_pool_hygiene():
    """池条目带 last_used 时间戳，30s 后被淘汰；每 host 最多 6 条。"""
    print("\n[F] 连接池条目 30s TTL/6 条容量上限/归还前校验")
    import tg as _tg

    be = _tg.TelegramBackend(slots=[{"token": "T", "chat_id": "C"}],
                              api_base="http://127.0.0.1:1", rate_limit=0)
    check("F1 _CONN_IDLE_MAX=30s 已生效", _tg._CONN_IDLE_MAX == 30.0,
          f"actual={_tg._CONN_IDLE_MAX}")
    check("F2 _CONN_POOL_MAX=6 已生效", _tg._CONN_POOL_MAX == 6,
          f"actual={_tg._CONN_POOL_MAX}")
    check("F3 _DEFAULT_HTTP_TIMEOUT=30 已生效", _tg._DEFAULT_HTTP_TIMEOUT == 30.0,
          f"actual={_tg._DEFAULT_HTTP_TIMEOUT}")
    sig = inspect.signature(be._open_conn)
    check("F4 _open_conn 接受 timeout 形参",
          "timeout" in sig.parameters,
          f"params={list(sig.parameters.keys())}")
    sig2 = inspect.signature(be._do_get)
    check("F5 _do_get 接受 timeout + force_new 形参",
          "timeout" in sig2.parameters and "force_new" in sig2.parameters,
          f"params={list(sig2.parameters.keys())}")

    # 验证连接池细节：模拟 8 条连接入池，应只保留 ≤ 6 条
    for _ in range(8):
        be._release_conn("http://127.0.0.1:1", object(), alive=True)
    key = be._conn_key("http://127.0.0.1:1")
    size = len(be._conn_pool.get(key, []))
    check("F6 单 host 连接数上限 ≤ 6", size <= 6, f"actual={size}")


# ============== G 组：保持公开 API 兼容 ==============
def test_g_api_compat():
    """iter_chunk / _stream_chunk 的对外签名必须保留兼容。"""
    print("\n[G] 公开签名兼容（iter_chunk / _stream_chunk 等）")
    import tg as _tg
    import webdav as _wd

    sig = inspect.signature(_tg.TelegramBackend.iter_chunk)
    params = list(sig.parameters.keys())
    expected = ["self", "file_id", "slot", "start", "end", "blk", "ctx"]
    check("G1 iter_chunk 签名不变",
          params == expected, f"got={params}")

    sig = inspect.signature(_wd.WebDAVHandler._stream_chunk)
    params = list(sig.parameters.keys())
    check("G2 _stream_chunk 加 expected_total 后仍有 blk 形参（向后兼容）",
          "expected_total" in params and "blk" in params,
          f"got={params}")


# ============== Main ==============
def main():
    test_a_pool_recovery()
    test_b_cl_too_high()
    test_b_truncate_at()
    test_c_no_double_send()
    test_d_keepalive_after_abort()
    test_e_blackhole_timeout()
    test_f_pool_hygiene()
    test_g_api_compat()
    ok = sum(1 for _, c, _ in results if c)
    total = len(results)
    print("\n" + "=" * 60)
    print(f"下载停止专项测试: {ok}/{total} 通过")
    print("=" * 60)
    if ok != total:
        print("\n失败项：")
        for n, c, e in results:
            if not c:
                print(f"  FAIL {n}  -> {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
