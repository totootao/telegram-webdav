# 超时日志增强报告

> 需求原文：「`172.17.0.1 - - [09/Sep/2026 02:16:58] Request timed out: TimeoutError('timed out')`
> 日志中有不少这个，**详细打印哪个访问超时了，后来有重试没，重试了几次**」

## 1. 这行日志为什么这么没用

它**不是本项目的代码打的**，而是 Python 标准库 `http.server` 打的：

```python
# CPython: http/server.py → BaseHTTPRequestHandler.handle_one_request
except TimeoutError as e:
    # a read or a write timed out. Discard this connection
    self.log_error("Request timed out: %r", e)   # ← 就这一行
    self.close_connection = True
    return
```

关键点：`TimeoutError` 在**基类内部**就被吞掉了，异常根本没抛出来，所以本项目覆写的
`handle_one_request` 里的 `except (ConnectionError, TimeoutError, socket.timeout)` **完全感知不到**——
想在外层补上下文是补不上的。日志里自然就只剩一个没有 method、没有 path、没有重试信息的空壳。

而且它把两种性质完全不同的事混在了一起：

| 性质 | 触发条件 | 该不该管 |
| --- | --- | --- |
| **keep-alive 空闲回收** | 客户端复用连接但迟迟不发下一条请求（`DAV_IDLE_TIMEOUT`，默认 30s） | **正常行为**，不该报警 |
| **请求处理中超时** | 读请求体 / 写响应卡住（`DAV_BODY_TIMEOUT`，默认 300s） | **真问题**，必须排查 |

日志里「有不少」的，绝大多数是前者（客户端开连接不复用 → 每 30s 回收一次刷一行）。

## 2. 改了什么

### 2.1 在 `log_error` 这一层拦截，补上上下文

`webdav.py` 新增 `WebDAVHandler.log_error()` 覆写：认出 `Request timed out` 就转给
`_on_request_timeout()`，其余照常走基类。这是唯一能拦到这条日志的层次。

### 2.2 用「请求行有没有解析出来」区分两种超时

新增 `parse_request()` 覆写：解析成功才置 `self._req_parsed = True`，同时建立本次请求的观测对象。

- 没解析出来 → 卡在「等下一条请求」= 空闲回收 → **默认静默**（`DAV_LOG_IDLE_TIMEOUT=on` 才打）
- 解析出来了 → 请求处理中超时 → **详细打印**

### 2.3 重试次数：新增加请求级观测对象 `_ReqCtx`

分片上传/下载跑在 `ThreadPoolExecutor` 里，**thread-local 传不进去**，所以显式往下传：

- 上传：`_one_retry` 每次重试 `ctx.bump_up()`（两处：并发上传 `_upload_parallel`、流式上传 `_upload_streaming`）
- 下载：`tg.iter_chunk(..., ctx=...)` 每次瞬断重试 `ctx.bump_down()`

这样重试次数能累计到「发起它的那条 WebDAV 请求」上，超时/结束日志才打印得出来。

### 2.4 「后来有没有重试」：按访问聚合的超时台账 `_TimeoutStats`

以 `(客户端IP, method, path)` 为键，30 分钟窗口内聚合：

- 超时发生 → `timeout += 1`
- 同一访问再来一次 → `attempt += 1`，并打印「此前已超时 N 次，这是第 M 次尝试」

### 2.5 顺手修的真 bug：`DAV_BODY_TIMEOUT` 配了不生效

`config.py` 里有 `body_timeout`（`DAV_BODY_TIMEOUT`，默认 300），但 handler 一直用模块常量
`_BODY_TIMEOUT`，**这个环境变量配了完全没用**，只能干等 300s。已改为从 config 取值（常量仅兜底）。

## 3. 现在的日志长什么样（真实容器实测）

```
# ① 请求处理中超时 —— 哪个访问、卡在哪、读了多少、重试几次、累计第几次
[webdav] 请求超时(连接已关闭,需重新发起): 客户端=172.17.0.1:45738 请求=PUT /demo_timeout/movie.mkv
  卡在=读取请求体 已读请求体=192.0KB/共4.8MB(4%) 已耗时=4.0s 重试情况=无重试(一次通过)
  | 该访问累计超时1次/客户端累计尝试3次
  | 重发 PUT 时已成功的分片会按 SHA 去重复用，只补传失败片
  | 底层=Request timed out: TimeoutError('timed out')

# ② 同一访问被重发 —— 直接回答「后来有重试没，重试了几次」
[webdav] 请求重传: 客户端=172.17.0.1 连接内第1个请求 PUT /demo_timeout/movie.mkv
  —— 该访问此前已超时 1 次，这是第 4 次尝试（已成功的分片会按 SHA 去重复用，只补传失败片）

# ③ keep-alive 空闲回收 —— 默认不打；DAV_LOG_IDLE_TIMEOUT=on 时：
[webdav] 连接空闲超时(正常回收,无需处理): 客户端=172.17.0.1:42026 等待下一条请求超 30s
  未收到数据，关闭连接 —— 上一个请求: PROPFIND / status=207
```

「重试情况」的长相：`无重试(一次通过)` / `上传分片重试2次` / `下载分片重试1次` / `上传分片重试2次、下载分片重试1次`。

## 4. 验证

### 4.1 新增 `timeout_log_test.py` —— 18/18 通过

```
[A] 请求处理中超时   A1~A7 全 OK（含 method+path / 阶段 / 已读进度 / 重试情况 / 累计次数 /
                    A7 标准库那行裸日志在 stderr 上已消失）
[B] keep-alive 空闲  B1 不被误报成「请求超时」  B2 不刷标准库日志（日志条数=0）
[C] 客户端重传       C1~C4 OK（识别出重传 / 此前超时次数 / 当前第几次 / 提示去重复用）
[D] 重试计数         D1~D5 OK（上传下载分别计数；用真实 TelegramBackend 打桩验证
                    下载瞬断重试确实 bump 了 ctx，且重试后数据 b"abcd" 完整不重复）
```

### 4.2 回归

| 测试 | 结果 |
| --- | --- |
| `prod_test.py`（真实 Telegram + 自建代理） | **17/17 通过** |
| `real_test.py`（本地假 Telegram，含完整性/断点续传） | **17/17 通过** |
| `timeout_log_test.py`（本次新增） | **18/18 通过** |
| `retry_unit_test.py`（分片重试） | 全部断言通过 |
| `dedup_test.py` / `dedup_partial_test.py` | 内容 SHA 一致、重传正常 |

## 5. 你的日志该怎么读

`172.17.0.1` 是 Docker 网桥网关地址 —— 说明**访问来自同一台机器上的另一个容器**
（实测本项目容器里看到的客户端 IP 也正是 `172.17.0.1`）。

升级后：

1. **那行裸日志消失**了。如果还有，说明跑的是旧镜像（`docker pull` + 重建容器）。
2. 若想确认「之前的那些到底是不是空闲回收」：加 `DAV_LOG_IDLE_TIMEOUT=on` 跑一会儿，
   看到的全是 `连接空闲超时(正常回收,无需处理)` → 就是客户端开连接不复用，**无需处理**，
   调大 `DAV_IDLE_TIMEOUT` 或直接让客户端别再用 keep-alive 都能让它彻底安静。
3. 看到 `请求超时(连接已关闭,需重新发起)` → 这就是真问题，日志里的
   `请求=PUT /xxx` + `卡在=读取请求体` + `已读请求体=A/共B(n%)` 直接指明是谁、卡在哪一步：
   - `卡在=读取请求体` 且进度长期不动 → 客户端/中间 Nginx 没把 body 发完（检查 `proxy_request_buffering off`）
   - `卡在=写出响应给客户端` → 客户端读得太慢或不读了（播放器拖进度条常见）
   - `重试情况=上传分片重试N次` → Telegram 侧在抖（429 / 代理瞬断），分片重试已兜住；
     真失败重传时按 SHA 去重复用，**只补传失败片**
