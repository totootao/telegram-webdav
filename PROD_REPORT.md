# 生产环境（真实 Telegram）测试报告

- 测试时间：2026-09-09 07:29:23
- 服务地址：`http://127.0.0.1:10010`（容器 tg-webdav，5 bot 池 + 自建 TG 代理）
- 主测试文件：`60MB`（按 20MB 分片 → 3 片，真实上传/下载）
- 卡顿阈值：单次等待 >300ms；严重卡顿 >1s
- 结论：**17/17 通过**

## 真实性能

| 指标 | 数值 |
|---|---|
| 上传 60MB | 6.63s (9.06 MB/s) |
| 全量下载 | 7.25s (8.27 MB/s) |
| 起播吞吐 | 23.83 MB/s |
| seek 吞吐 | 17.62 MB/s |
| 平均吞吐 | 16.05 MB/s |

## 优化前后对比（本轮新增 `file_path` 并发预取）

真实环境里**一次 `getFile` 要 ~3 秒**（自建代理 RTT）。优化前每个分片串行各等一次
getFile，3 片文件的首次下载光等 getFile 就耗掉约 9s。优化后在**首片下载的同时**后台
并发预取其余分片的 `file_path`，把这段 RTT 完全藏进首片下载时间里。

| 指标 | 优化前 | 优化后 | 提升 |
|---|---|---|---|
| 全量下载 60MB(3片，缓存冷) | 17.02s | **7.25s** | **快 2.35×** |
| 全量吞吐 | 3.52 MB/s | **8.27 MB/s** | **+135%** |
| 分片1 耗时 | ~6.0s(含 getFile 3s) | **0.836s** | -86% |
| 分片2 耗时 | ~5.0s(含 getFile 4s) | **0.814s** | -84% |
| 分片间隔 | 0.000s | 0.000s | 持平(均已无缝) |
| 上传 60MB | 8.12s | 6.63s | +18%(同链路波动范围内) |

日志佐证（预取与首片下载并行，`getFile` 并发而非串行）：
```
07:26:04  file_path 预取开始: 分片数=2
07:26:07  getFile 预取成功: ...part002 耗时=3.200s
07:26:07  getFile 预取成功: ...part001 耗时=3.255s   ← 两片并发，总 3.256s(非串行 6.5s)
07:26:07  file_path 预取完成: 分片数=2 耗时=3.256s
07:26:09  GET 分片[0](首) 流式完成: 耗时=5.586s      ← 预取已藏在首片下载里
07:26:10  GET 分片[1] 流式完成: 耗时=0.836s 分片间隔=0.000s
07:26:11  GET 分片[2] 流式完成: 耗时=0.814s 分片间隔=0.000s
```

### 仍存在的瓶颈：首片 TTFB ≈ 4.5s

首片的 `getFile`(~3s) + Range 首字节(~1.5s) 在关键路径上，预取无法消除首片自身的
getFile。后续若想进一步降低首播等待，可考虑：
1. 代理侧优化 `getFile` 响应（3s 明显偏高，可能是回源 Telegram 官方 API 的 RTT）；
2. 把 `file_path` 缓存持久化（当前仅内存，服务重启后失效，50min TTL）；
3. 上传时就把 `file_path` 一并落库，下载时直接省掉 getFile（改动较大但收益最直接）。

## 播放平滑度（真实带宽）

| 场景 | TTFB | 最大等待 | p95 | 卡顿>300ms | 严重>1s | 吞吐 |
|---|---|---|---|---|---|---|
| 起播 bytes=0- | 343ms | 338ms | 4ms | 2次 | 0次 | 23.83MB/s |
| seek 60% | 336ms | 494ms | 2ms | 1次 | 0次 | 17.62MB/s |
| 全量(无Range) | 4578ms | 338ms | 6ms | 2次 | 0次 | 8.27MB/s |

## 详细用例

| 用例 | 结果 | 说明 |
|---|---|---|
| auth.required(401) | PASS | status=401 |
| propfind.root(207) | PASS | status=207 |
| mkcol(201/405已存在) | PASS | status=405 |
| put.big(201/204) | PASS | status=201 |
| propfind.size_matches | PASS | status=207 |
| get.big.full(200) | PASS | status=200 |
| get.big.sha256_identical | PASS | len=62914560/62914560 |
| get.big.bytes_identical | PASS |  |
| get.range.head(206) | PASS | status=206 len=1048576 |
| get.range.mid_cross(206) | PASS | status=206 len=2097152 |
| get.range.tail(206) | PASS | status=206 len=2097152 |
| playback.start(206) | PASS | status=206 |
| playback.start.bytes_ok | PASS | len=62914560 |
| playback.seek(206) | PASS | status=206 len=25165824 |
| concurrent.ranges.all_206 | PASS | ok=4/4 err={} wall=3.20s |
| resume.from_half(206) | PASS | status=206 len=31457280 |
| get.empty(200_zero) | PASS | status=200 len=0 |
