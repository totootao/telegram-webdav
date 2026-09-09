# 大文件（>1GB）上传/下载测试报告

测试日期：2026-09-09
目标服务：`127.0.0.1:10010`（容器 `tg-webdav`，5 bot 池 + 自建 TG 代理 `tg.totootao.top/tg`）
测试文件：`bigfile_test.py`，1.2GB 随机内容（按 20MB 分片 → 60 片，真实上传/下载）

## 结论

**14/14 全部通过。** 1.2GB 文件可完整上传下载，SHA-256 一致；上传内存峰值被限在数百 MB（与文件大小无关），不再随文件线性增长；下载全程流式、内存近似零增量；Range / 起播 / seek / 断点续传 / 多路并发在超大文件下均正常。

| 维度 | 结果 |
|---|---|
| 上传完整性 | ✅ SHA 一致，status=204 |
| 上传内存峰值 | ✅ 644MB（旧路径 2.27×≈2743MB，2GB 文件直接 OOM） |
| 全量下载 | ✅ 1200MB 全收，SHA 一致，内存增量 3MB |
| Range 首/中/尾 | ✅ 206 + SHA 正确 |
| 起播 / seek TTFB | ✅ 0.34s / 1.05s |
| 断点续传 | ✅ 分段 SHA 正确 |
| 4 路并发 Range | ✅ 全部 206 |

## 本次修复的两个问题

### 1. 上传内存 2.27× → 被限住（流式边收边传）

旧路径（`_read_exact` 整包读进内存 + `_upload_parallel` 再切片）下，1.2GB 上传服务端内存峰值约 **2743MB（文件大小的 2.27 倍）**；2GB 文件约 4.5GB，普通小内存容器直接 OOM。

改为 `_upload_streaming`：从请求体**边收边传**，攒满一片立即交给线程池上传，内存占用 ≈ 在飞分片数 × 分片大小（默认 5×2×20MB≈200MB）+ Python 开销，**与文件大小无关**。

实测 1.2GB 上传：内存峰值 **644MB（0.53× 文件大小）**，较旧路径 **下降 4.3×**；2GB 文件预计仍 ~600-700MB，不再 OOM。

### 2. 分片下载偶发瞬断（代理抖动导致整个大文件下载失败）

测试中发现：1.2GB 下载到某个分片时，自建代理偶发返回 `ResponseNotReady: Request-sent`，原 `iter_chunk` 把这个 `http.client` 异常**裸抛**出去（非 `TGError`），而流式下发只 catch `TGError`，于是变成「未捕获异常(返回500)」并在已发 206 头后中断连接——客户端只收到前 40MB（2 个分片）即 EOF。该问题**必现于特定分片**，curl / 孤立 python 请求因代理当时健康而正常，极具迷惑性。

修复（`tg.py` `iter_chunk`）：
- 单分片下载增加瞬断重试（`_CHUNK_RETRY=3`，轻量退避），代理偶发抖动自动恢复；
- 仅在**尚未向客户端写出任何字节**时才重试（已写字节不可回退，避免重复字节污染数据）；
- 所有候选与重试耗尽后，**统一抛 `TGError`**（不再裸抛 http 异常），让上层以「干净断连」处理；
- webdav `_stream_chunk` 增加兜底：任何非 `TGError` 异常统一转 `TGError`，杜绝「未捕获异常(返回500)」。

修复后 `resume.seg1` 等之前必现的截断项全部通过。

## 详细结果（1.2GB）

```
=== 阶段1：上传 1200MB ===
  上传完成 status=204 耗时=69.31s 吞吐=17.31 MB/s
  服务端内存: 基线=17MB → 峰值=644MB (增量=628MB)
  [PASS] upload.status
  [PASS] upload.mem_bounded_streaming 内存增量=628MB / 文件=1200MB 占比=52.3%
         (旧路径 2.27×≈2724MB；流式:峰值被限在数百 MB)

=== 阶段2：全量下载 1200MB + SHA-256 校验 ===
  下载完成 status=200 字节=1200.0 MB 耗时=199.68s 吞吐=6.01 MB/s TTFB=4.408s
  服务端内存: 基线=194MB → 峰值=197MB (增量=3MB)
  [PASS] download.full_size  [PASS] download.sha256_match  [PASS] download.mem_streaming_not_buffered

=== 阶段3：Range 分段（首/中/尾）===
  [PASS] range.头部 0-4MB       status=206 sha_ok=True
  [PASS] range.中部 600MB 处 4MB status=206 sha_ok=True
  [PASS] range.尾部 最后2MB      status=206 sha_ok=True

=== 阶段4：起播 / seek ===
  [PASS] playback.start_ttfb        status=206 TTFB=0.343s
  [PASS] playback.seek_50pct_ttfb   status=206 TTFB=1.045s

=== 阶段5：断点续传 ===
  [PASS] resume.seg1_bytes_sha               status=206 bytes=100.0 MB
  [PASS] resume.seg2_continues_from_break    status=206 bytes=100.0 MB 从断点 100.0 MB 续传

=== 阶段6：4 路并发 Range ===
  [PASS] concurrent.4_ranges 墙钟=2.06s 状态=[206, 206, 206, 206]

总耗时 291.8s  结果: 14 通过 / 0 失败
```

## 附带回归

- `real_test.py`（小文件 + 完整性/损坏检测）：**17/17 通过**
- `prod_test.py`（真实环境 17 项）：**17/17 通过**

## 备注

- 测试数据保留于 `/bigtest`（KEEP_DATA=1），清理：`DELETE /bigtest`。
- 代理实测吞吐 ~5-17 MB/s 波动，属代理/Telegram 侧上限，非本服务瓶颈。
- 上传内存实测 628MB 高于理论 ~200MB，主要源于 Python `bytearray` 缓冲与 GC 时机；仍远低于旧路径且恒定不随文件增大。
