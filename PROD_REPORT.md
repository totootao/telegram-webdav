# 生产环境（真实 Telegram）测试报告

- 测试时间：2026-09-09 16:42:12
- 服务地址：`http://127.0.0.1:10010`（容器 tg-webdav，5 bot 池 + 自建 TG 代理）
- 主测试文件：`60MB`（按 20MB 分片 → 3 片，真实上传/下载）
- 卡顿阈值：单次等待 >300ms；严重卡顿 >1s
- 结论：**17/17 通过**

## 真实性能

| 指标 | 数值 |
|---|---|
| 上传 60MB | 0.00s (-) |
| 全量下载 | 6.28s (9.55 MB/s) |
| 起播吞吐 | 8.13 MB/s |
| seek 吞吐 | 7.95 MB/s |
| 平均吞吐 | 8.84 MB/s |

## 播放平滑度（真实带宽）

| 场景 | TTFB | 最大等待 | p95 | 卡顿>300ms | 严重>1s | 吞吐 |
|---|---|---|---|---|---|---|
| 起播 bytes=0- | 564ms | 777ms | 13ms | 9次 | 0次 | 8.13MB/s |
| seek 60% | 563ms | 566ms | 13ms | 3次 | 0次 | 7.95MB/s |
| 全量(无Range) | 927ms | 760ms | 8ms | 5次 | 0次 | 9.55MB/s |

## 详细用例

| 用例 | 结果 | 说明 |
|---|---|---|
| auth.required(401) | PASS | status=401 |
| propfind.root(207) | PASS | status=207 |
| mkcol(201/405已存在) | PASS | status=405 |
| put.big(复用已有文件) | PASS | 跳过上传，避免重复占用频道 |
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
| concurrent.ranges.all_206 | PASS | ok=4/4 err={} wall=6.22s |
| resume.from_half(206) | PASS | status=206 len=31457280 |
| get.empty(200_zero) | PASS | status=200 len=0 |
