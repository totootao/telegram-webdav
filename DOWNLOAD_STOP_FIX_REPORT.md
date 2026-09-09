# 下载过程随机停止 bug 修复报告

> 仓库：https://github.com/totootao/telegram-webdav
> 修复分支：main（commit `0b836fb`）
> 影响范围：所有经过代理 / 多分片下载的 GET 请求

---

## 一、问题现象

下载大文件（视频、压缩包）时**下载过程随机停止**，表现不一：

- 客户端已收到 `206 Partial Content` 头、也下到了一部分字节，但**中途连接突然断开**，剩余字节永远不来；
- 播放器 / 下载器卡在某一个分片边界，进度条不动，重试才继续；
- 偶发的 `http.client.ResponseNotReady` / `BadStatusLine` 直接冒泡成未捕获异常，服务端返回 `500` 并污染已发出的响应体；
- 极少数情况拿到**看似下载成功、实则被截断/字节错位**的文件（数据静默损坏，最危险）。

根因不是单一一处，而是**连接池复用 + 下载主路径重试 + 写出侧**三处叠加的脆弱性。

---

## 二、根因分析（按优先级）

### P0 — 已向客户端写出字节后，仍切换代理候选（导致重复字节污染）
`tg.iter_chunk` 在遍历各代理候选时，若某候选已在 `try` 块内通过 `yield b` 向客户端写出过字节、随后 `resp.read()` 抛瞬断异常，原逻辑会 `break` 出当前候选、进入**下一个候选重试**。新候选从分片起点重新下载并再次 `yield` —— 客户端收到的字节流里，前半段是候选 A、后半段是候选 B 从头重发，Content-Length 帧错位、数据重复损坏，表现为"下载到一半就坏掉"。

**修复**：`total_yield > 0` 时不再切候选，直接抛 `TGError`，连接干净中断（客户端看到的是一次干净的下载失败，而非损坏数据）。

### P1 — keep-alive 连接池用 `will_close` 判断"可复用"不可靠（假活连接）
原 `_do_get` / `_release_conn` 用 `resp.will_close` 决定是否把连接还回池里。但 `will_close` 只反映**响应头里 Connection 字段**，并不能保证服务端真的还保持这条 TCP 连接。经自建代理时，代理侧随时可能 `FIN/RST` 掉 keep-alive 连接，连接被还回池、下次复用就触发 `BadStatusLine` / `ResponseNotReady` / `RemoteDisconnected`。

**修复**：
- 归还前用 `conn.is_closed` / `sock` 状态**实测连接是否真的还活着**，死连接直接关掉不进池；
- 连接池新增 **TTL（`_CONN_IDLE_MAX=30s`）** 与 **容量上限（`_CONN_POOL_MAX=6`）**，超过即淘汰最旧/最老的，避免"假活"连接长期滞留；
- `iter_chunk` 的 `finally` 归还时若本次读取中途抛过异常，按"连接已损坏"处理，不按 `will_close` 盲目归还。

### P2 — 代理少发字节被静默当成下载成功（数据静默损坏）
`iter_chunk` 的 `while True: b = resp.read(blk)` 在 `b == b""` 时直接 `break`，**不校验实际收到的字节数是否等于代理声明的 `Content-Length`**。代理若中途断流（声明发 20MB 实际只发 5MB 就 `FIN`），`read()` 返回空即结束，上层以为整片下载成功 → 文件被截断却不报错，最危险的一类。

**修复**：
- `iter_chunk` 增加 `expected_total`（从 `Content-Length`/`Content-Range` 解析），读完若 `total_yield != expected_total` 直接抛 `TGError`；
- `_stream_chunk` 增加 `expected_total` 参数，写出阶段按分片应发字节数兜底校验，超出/不足都记为异常；
- `_serve_file` 收尾断言 `sent == total`，杜绝"发了 206 头却少发字节"的响应污染。

### P3 — 黑洞代理 / 超时形参失效（一次失败挂死 18 分钟）
- `_do_get` 的 `timeout` 形参原本是死参数（实际永远是 `_open_conn` 硬编码的 `180s`）；
- `_do_get` 内部 `for _ in range(2)` 的第二次重试**仍从池里取**（可能还是那条坏连接），没有"强制新建连接"的逃生通道；
- 黑洞代理（建连成功但永远不返回 body）会让单次请求卡满 `180s`，叠加 `_CHUNK_RETRY` 与多候选就是十几分钟。

**修复**：
- `timeout` 真正生效（默认 `_DEFAULT_HTTP_TIMEOUT=30s`）；
- 新增 `force_new=True` 路径：失败一次后强制走 `_open_conn` 拿全新 TCP；
- 黑洞场景从"最多挂 18min"收敛到"单次请求 ≤ timeout 即报错并换候选/重试"。

### P4 — 写出超时误判 + 滑动窗口预取收尾不取消
- `_stream_chunk` 把 `socket.timeout` 一律当成"客户端已断开"，但**服务端写超时**也会被误判，导致正常下载被当成客户端 abort 而提前终止；
- 滑动窗口预取线程池 `pf.shutdown(wait=False)` 未传 `cancel_futures`，Python 3.9+ 下已排队的预取任务会占住代理并发直至完成，可能拖慢收尾。

**修复**：写出超时细分为 `_ClientGoneEarly`（客户端先断）与 `socket.timeout`（服务端写超时），后者不误杀下载；`pf.shutdown(wait=False, cancel_futures=True)`。

---

## 三、改动文件

| 文件 | 改动 |
|---|---|
| `tg.py` | P0/P1/P3 核心修复：`_open_conn` 接受 `timeout`；连接池加 TTL/容量/存活校验；`_do_get` 加 `timeout`+`force_new`；`_get_file_path` 读完 body 再判 alive + `finally` 释放；`iter_chunk` 加 `expected_total` 校验、已写字节不切候选、`finally` 异常时按坏连接处理；新增 `_ClientGoneEarly` 异常 |
| `webdav.py` | P2/P4：`_stream_chunk` 加 `expected_total` + 写出超时细分；`_serve_file` 收尾断言 `sent == total`；预取线程池 `cancel_futures=True` |
| `fake_telegram.py` | 测试用故障注入钩子：`send_connection_close_after` / `truncate_at` / `wrong_content_length` / `black_hole` |
| `download_stop_test.py` | **新增** 16 个用例，覆盖 5 类场景 |
| `TEST_REPORT.md` | 测试报告时间戳/数值更新 |

---

## 四、验证结果

### 4.1 单元测试（`download_stop_test.py`，16/16 全过）

| 分组 | 场景 | 结果 |
|---|---|---|
| A | 池污染后自愈（服务端主动 FIN 后复用检测） | A1/A2 PASS |
| B | `Content-Length` 虚高（声明 4MB 实发 2MB）→ 不再静默成功 | B1/B'1 PASS |
| C | 已写字节后切候选 → 不再重复下发字节 | C1 PASS |
| D | 客户端中途 abort 后，下一次完整 GET 仍 200 且内容一致（keep-alive 干净） | D1 PASS |
| E | 黑洞代理在 <10s 内报错、不返回"完整截断"成功 | E1/E2 PASS |
| F | 连接池常量生效（`_CONN_IDLE_MAX=30s` / `_CONN_POOL_MAX=6` / `_DEFAULT_HTTP_TIMEOUT=30s`） | F1–F6 PASS |
| G | 公开接口签名兼容（`iter_chunk` / `_stream_chunk`） | G1/G2 PASS |

### 4.2 真实场景端到端测试（Docker，`totootao/telegram-webdav:latest` 最新代码）

用真实 Telegram bot + 3 个真实代理，上传 **45MB 文件**（分 3 片），再做下载验证：

| 用例 | 结果 |
|---|---|
| 完整下载 ×3（共 3 次，每次 45MB） | 全部 `200`，SHA-256 与源文件**逐字节一致** |
| Range 下载 `bytes=10000000-20000000` | `206`，10000001 字节，内容切片**完全一致** |
| 5 路并发下载 | 全部 `200`，SHA 全部一致（连接池压力下无随机停止） |
| 分片衔接 | 最大分片间隔 `0.000s`（平滑衔接，无卡顿断点） |

> 测试完成后已 `DELETE` 测试文件并清理沙箱容器，未遗留脏数据。

### 4.3 回归测试

| 测试套件 | 结果 |
|---|---|
| `selftest.py` | 70/71（1 例为仓库既有、与本次改动无关的用例，原基线即如此） |
| `real_test.py` | 17/17 |
| `proxy_failover_test.py` | 7/7 |

---

### 4.4 网络抖动端到端测试（chaos 反代注入真实流量）

新增 `chaos_proxy.py`：在 tg.py 与真实 Telegram 代理之间做本地反向代理，对每个请求随机注入
`connfail`（直接断连，模拟代理瞬断）/ `delay`（随机延迟）/ `earlydrop`（发出响应头后 0 body 立即断，模拟首字节前断流）/ `middrop`（转发部分 body 后断，模拟读取中途断流），并在 `/__chaos_stats` 暴露注入计数。

| 场景 | 配置 | 结果 |
|---|---|---|
| 上传抗抖动 | 极端抖动（connfail40%+delay30%+earlydrop30%）+ 单候选 | `201`，43s；日志可见 `RemoteDisconnected`/`IncompleteRead` 触发 `attempt=1/3`、`2/3` 重试自愈 |
| 下载·温和抖动+多候选 | connfail15%+delay15%+earlydrop10% + **chaos+2 真实代理共 3 候选** | 完整下载 ×3 + 5 路并发 **8/8 全部 `200` 且 SHA 一致**（chaos 统计：34 请求中 11 次触发抖动，含 1 次 `瞬断重试(1/3)` 后自愈） |
| 下载·极端单候选 | connfail40%+delay30%+earlydrop30% + 单候选 | 完整下载 ×3 全部成功；5 路并发 3/5 成功、2 路在「已发 200 头后」因单分片重试 3 次仍失败而断连（拿到截断的 200，但**不损坏数据**） |

**结论**：
- 修复后，温和/真实强度网络抖动下，下载能稳定完成（不随机停止），靠的是 P0/P1/P3 的「瞬断重试 + 连接池自愈 + 多代理候选切换」。
- 在**极端持续抖动 + 仅单代理候选**的退化场景，HTTP 流式响应一旦发出 `200` 头便无法反悔：若某分片在「已写出字节后」遇到不可重试故障，只能中断连接，客户端会拿到「截断的 200」（文件不全，但**绝不返回损坏字节**——这正是 P2 修复的价值，宁可失败也不静默损坏）。持续抖动期内单分片 3 次重试可能仍不够。
- 生产建议：**始终配置 ≥2 个代理候选**（仓库默认的 `TG_PROXY_POOLS` 已配 3 个），单个代理抖动时自动切候选兜底，下载不会停。若需更强韧性，可把 `_CHUNK_RETRY` 调高或加入「整请求 Range 续传」。

---

## 五、部署

代码已推送至 `main`。若通过 Docker 运行，需**重新构建镜像**（仓库 `latest` 标签仍是旧代码）：

```bash
cd telegram-webdav
docker build -t totootao/telegram-webdav:latest .
# 然后用原有 docker run 命令启动即可（环境变量不变）
```

> 安全提示：原 `docker run` 命令把 `DAV_PASSWORD`、bot `token` 明文写在命令行，建议改放到 `.env` 文件 + `--env-file` 加载，避免进入 shell 历史。
