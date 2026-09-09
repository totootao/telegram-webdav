# TelegramWebDAV

> 用 Telegram 频道 / 群组当存储后端的**纯 WebDAV 服务**。
> 文件字节分片存进 Telegram，文件树元数据存进 **SQLite**。
> 零第三方依赖（仅 Python 标准库），可直接被 Windows 映射驱动器 / macOS Finder / rclone / cadaver 挂载。

参考项目 [otterhub-server](https://github.com/totootao/otterhub-server) 的 Telegram 交互方式实现，并将其"Cloudflare KV + 前端上传"架构，替换为"SQLite + 标准 WebDAV 协议"。

---

## 它和 otterhub-server 的关系

otterhub-server 用 **Cloudflare KV** 存元数据、用 **Telegram Bot API** 存字节，前端通过自定义接口分片上传。
本项目沿用它**与 Telegram 交互的核心逻辑**，但换了一套更通用的访问层：

| 关注点 | otterhub-server | 本项目 (TelegramWebDAV) |
| --- | --- | --- |
| 元数据 | Cloudflare KV | **SQLite**（`nodes` 表） |
| 字节存储 | Telegram 频道/群组（sendDocument） | 同左 |
| file_id 与 bot 绑定 | ✅ 记录 `tgSlot`，下载用对应 bot | ✅ 记录 `slot`，下载用对应 bot token |
| 分片 | 20MB / 片，每片独立 `file_id` | 同左（`CHUNK_SIZE_MB`，默认 20，最大 50） |
| 下载 | `getFile` → `file_path` → Range 流式拼接 | 同左，支持 HTTP Range |
| 限流 | 1 msg/s，429 换 bot 槽位 | 同左（多 bot 池 + 退避） |
| 访问协议 | 自有 HTTP API + 网页前端 | **标准 WebDAV**（PROPFIND/GET/PUT/…） |
| 频道自动入库 | Telegram Webhook 提取 `file_id` | 同左（`/telegram/webhook`） |
| 空文件 | 1 字节占位打标 | 同左（直接返回空体） |

也就是说：**Telegram 那一半完全对齐 otterhub，访问层从"私有 API"升级为"任意 WebDAV 客户端都能用"**。

---

## 工作原理

```
WebDAV 客户端 ──PROPFIND/GET/PUT──▶ TelegramWebDAV (本服务)
                                        │
                          ┌─────────────┼─────────────────────┐
                          │             │                     │
                     SQLite 元数据   Telegram 上传         Telegram 下载
                     (nodes 表)    sendDocument→file_id   getFile→file_path→Range
```

- **PUT（上传）**：按 `CHUNK_SIZE_MB` 切片，**流式**读请求体，每片 `sendDocument` 发到频道，拿到 `file_id`；
  把 `{file_id, slot, size, message_id, sha256}` 数组写进 SQLite（同时算出整文件 SHA-256）。
  空文件用 1 字节占位并打标。
- **GET（下载）**：从 SQLite 读出分片索引，按请求的 `Range` 计算覆盖哪些分片，
  逐片向 Telegram 发 `Range` 请求并**流式拼接**返回（支持视频拖拽 / 断点续传），
  同时**逐片校验 SHA-256**（见下节）。
- **目录 / 移动 / 复制 / 删除**：纯 SQLite 文件树操作。COPY 共享同一批 `file_id`（物理不重复上传）；
  DELETE 只删元数据——Telegram 不支持删已发消息，物理分片仍留频道（与 otterhub 一致）。

---

## 数据完整性（SHA-256 端到端校验）

分片存进 Telegram、再按 `file_id` 拼回来，"字节是否还是原来那些"必须有据可查，不能靠信任。
本服务在**上传时记录哈希、下载时逐片校验**：

| 阶段 | 做什么 |
|---|---|
| **PUT** | 边读请求体边增量算**整文件 SHA-256**（`hashlib` 增量 `update()`，不额外占内存）；每个分片再单独算一次 SHA-256 |
| **落库** | 整文件哈希存 `nodes.file_hash`；每片哈希存进 `chunks` 数组的 `sha256` 字段 |
| **GET** | 流式下发的同时对**完整落在请求区间内**的分片重算 SHA-256，到分片边界比对 |
| **异常** | 哈希不符、或实际收到的字节数 ≠ 记录的 `csize`（Telegram 返回截断）→ 立刻**中断连接**并打日志，绝不把错数据当完整文件交给客户端 |

几个刻意的设计取舍：

- **逐片校验而非整文件重算**：整文件校验必须先缓冲全文才能算哈希，会直接废掉流式下发
  （大文件撑爆内存、Range 请求被迫重建整个文件）。逐片校验内存恒定、Range 只校验命中的分片。
- **Range 截断的分片不校验**：`Range` 把某个分片切开时只能拿到局部字节，没有完整分片可比，
  此时只流式转发不校验（该分片会在后续完整请求里被校验到）。
- **向后兼容**：旧版本上传的文件没有 `sha256` 字段，GET 时自动跳过校验（不会误报）；
  重新 PUT 一次即纳入保护。
- **COPY / MOVE 零成本**：哈希跟着元数据走，不重算字节。
- **性能**：`sha256` 是 C 实现（现代 CPU 有 SHA-NI，吞吐数百 MB/s），远快于 Telegram 网络 I/O，
  与网络等待并行，墙钟时间基本无感；额外存储约每分片 64 字节 hex。

日志关键字：分片损坏时服务端会打印
`[webdav] 分片完整性校验失败 <路径> @offset <偏移>: 内容不符或被截断`。

---

## 媒体时长记录（音频 / 视频）

上传音频/视频时，服务会在**本地**解析出时长存进元数据，并可通过 WebDAV 自定义属性读到。

> **为什么不交给 Telegram**：`sendAudio` / `sendVideo` 确实会解析媒体并回带 `duration`，
> 但 `sendVideo` 会**转码**视频——下载到的字节 ≠ 上传的字节，直接破坏上面的 SHA-256 校验；
> 而且文件切成 20MB 分片后，每片都是任意字节片段，根本不是合法媒体文件，无从解析。
> 所以时长在本地算，Telegram 侧仍是 `application/octet-stream` 原字节，存储链路一行不改。

| 格式 | 取时长的依据 | 精度 |
|---|---|---|
| MP4 / M4A / M4B / M4V / MOV / 3GP | `moov` → `mvhd` 的 timescale / duration | 精确 |
| MKV / WebM | EBML：`Segment` → `Info` 的 `TimestampScale` × `Duration` | 精确 |
| OGG（Opus） | 末页 granule（恒按 48kHz 计）减 `pre_skip` | 精确 |
| OGG（Vorbis） | 末页 granule（PCM 采样数）÷ ID header 的采样率 | 精确 |
| MP3 | Xing / Info 头的总帧数；无 Xing 时按 CBR 比特率估算 | 精确 / 估算 |
| WAV | `fmt ` 的字节率 + `data` 块大小 | 精确 |
| FLAC | STREAMINFO 的 sample rate 与 total samples | 精确 |

两个容易踩的坑（已在代码里处理）：

- **MKV 的 `Duration` 不是秒**，而是以 `TimestampScale` 为单位的计数值，必须换算
  `秒 = Duration × TimestampScale / 1e9`。
- **OGG 的 granule 单位是 codec 相关的**：Opus 恒定按 48kHz 计（与输入采样率无关）
  且要减掉 `pre_skip`，Vorbis 则是 PCM 采样数 ÷ 采样率。认错 codec 会差出数量级。

**开销**：只在 PUT 时取**首片头部 512KB + 末片尾部 4MB** 做采样，O(采样) 解析，
不全文扫描；解析失败一律返回 `None`，绝不影响上传结果。相比分片上传的网络 I/O 可忽略。

**读取**：PROPFIND 响应里带自定义命名空间属性（单位秒）：

```xml
<T:duration>123.456</T:duration>
```

命名空间为 `urn:telegram-webdav:meta`，不支持该属性的客户端会自动忽略。

### 音视频时长日志

PUT 上传时会打印解析结果（类型 / 容器 / 可读时长）：

```
[webdav] PUT 媒体解析: path=/movie/a.mp4 类型=video 容器=mp4 时长=1:23:43.678(5023.678s)
[webdav] PUT 完成: path=/movie/a.mp4 状态=新建(201) size=1.2GB(1312345678B) 分片数=61
         content_type=video/mp4 时长=1:23:43.678(5023.678s) 耗时=42.31s 吞吐=29.4 MB/s file_hash=有
```

GET / 播放时同样带时长，并附上本次请求的耗时与吞吐：

```
[webdav] GET 开始: path=/movie/a.mp4 size=1.2GB 分片数=61 Range=bytes=0- 类型=video 时长=1:23:43.678(5023.678s)
[webdav] GET 完成: path=/movie/a.mp4 状态=206 区间=0-1312345677/1312345678 已发=1.2GB(1312345678B)
         耗时=38.02s 吞吐=32.9 MB/s 并发=5 类型=video 时长=1:23:43.678
```

- `类型` 由 Content-Type 或扩展名判断（`video` / `audio` / `-` 表示非媒体）。
- `容器` 是实际解析成功的封装（mp4 / mkv / webm / mp3 / flac / wav / ogg），
  用来确认走的是哪条解析分支。
- 媒体文件但**解析不出时长**时会明确打印
  `未能解析出时长(采样不足/非标准封装/加密moov)`，与非媒体文件（`时长=-`）区分开。
- `耗时` / `吞吐` 用于定位慢在哪：吞吐低说明瓶颈在网络或代理；耗时高但吞吐正常说明是连接建立慢。

**已知局限**：

- **OGG 视频（Theora）不支持**：它的 granule 编码了帧号与关键帧偏移，换算规则另有一套，返回 `None`
- **直播录制的 WebM/MKV 常无 `Duration`**（流式写入来不及回填），这类返回 `None`；
  要覆盖只能扫到最后一个 Cluster 的 timestamp 反推，代价大，暂不做
- MP4 若未做 faststart 且 `moov` 大于 4MB，尾部采样可能覆盖不到 → 返回 `None`
- VBR MP3 若无 Xing / Info 头，只能按平均比特率估算，存在误差
- 已入库的旧文件没有时长，重新 PUT 一次即可补上

---

## 音视频播放优化（丝滑播放）

播放器（VLC / Infuse / nPlayer / 浏览器）感受的不是"总吞吐"，而是**数据到达的平滑度**。
本服务针对播放场景做过一轮专门优化：

### 问题：分片边界的周期性卡顿

早期实现里，**只有首片是边下边发**，其余分片要 `b"".join()` 整片（20MB）缓冲到内存后再一次性
写出。在真实带宽下这就是灾难——假设 Telegram 下载速度 12MB/s，一个 20MB 分片要 1.7s，播放器
就会经历：

```
流畅收到 20MB → 干等 1.7s → 突然收到 20MB → 干等 1.7s → …
```

文件有多少个分片，播放就卡多少次。

### 方案：全分片流式下发

1. **所有分片都边下边发**（`TG_STREAM_ALL_CHUNKS`，默认 `on`）：从 Telegram 读到一块就立刻
   写给客户端，数据连续流动，分片边界不再有停顿。
   > 这只有在「单文件单线程串行下载」之后才可能做到——并发下载时其余分片无序，必须先缓冲。
2. **读块 256KB**（`TG_STREAM_BLOCK_KB`）：起播 / seek 的首字节更快，数据到达更平滑。

SHA-256 **仍然逐片校验**：边发边增量计算，到分片末尾比对，不符立刻中断连接。
唯一取舍是流式无法「先验后发」——这是流式下发的固有代价（缓冲校验则必然卡顿）。

### 实测（60MB / 3 分片，限速 12MB/s 模拟真实带宽）

| 场景 | 旧(仅首片流式) | 新(全分片流式) | 改善 |
| --- | --- | --- | --- |
| 起播 `bytes=0-` | 最大等待 **1750ms**（卡顿 2 次） | **21ms**（0 次） | 下降 98.8% |
| seek 中部 `bytes=60%-` | 最大等待 **1707ms**（卡顿 1 次） | **21ms**（0 次） | 下降 98.7% |
| 尾部 seek `bytes=-2MB` | 21ms | 21ms | 持平（只命中 1 片） |

> 复现：`python3 playback_test.py`，报告见 `PLAYBACK_REPORT.md`。

### 调优建议

- 播放卡顿 / 起播慢：确认 `TG_STREAM_ALL_CHUNKS=on`，并把 `TG_STREAM_BLOCK_KB` 调到 128。
- 纯大文件下载（不看视频）：`TG_STREAM_BLOCK_KB=1024` 可减少系统调用开销。
- 分片太大（`CHUNK_SIZE_MB`）会让单片下载时间变长，播放场景下建议 10~20MB。

---

## 首字节加速：file_path 预热（不落库）

下载一个文件前必须先调 Telegram `getFile` 换 `file_path`。实测这一步要 **~0.7~1.0s**，
占首片 TTFB 的 40%~85%——用户点开文件后干等的，主要就是它。

### 为什么不落库

`file_path` **有效期只有 1 小时**，写进数据库过一会儿再读就是过期路径，反而会下载失败。
现有方案是**内存缓存（TTL 50 分钟）+ 提前预热**：不碰数据库，只让 getFile 提前发生。

### 预热时机

| 时机 | 行为 | 覆盖场景 |
| --- | --- | --- |
| **PROPFIND 列目录** | 后台预取目录内各文件的**首片** file_path | 客户端列目录 → 点开文件（绝大多数场景） |
| **HEAD 探测** | 后台预取该文件的**全部分片** | 播放器/下载器探测后紧跟 GET |

- 预热在响应发出**之后**由 daemon 线程执行，**不阻塞**列目录（实测 PROPFIND 仍 2.5ms 返回）。
- 走独立连接，不与下载争用 keep-alive 连接池；已缓存的分片自动跳过（零开销）。
- 失败静默，不影响主流程（下载时会自动回退到常规 getFile）。

### 实测（真实 Telegram + 自建代理，每场景 3 轮取中位数）

| 场景 | 首片 TTFB | 相对基线 | 首片 getFile 未命中 |
| --- | --- | --- | --- |
| 冷启动直接 GET（基线） | **1.803s** | — | 3/3 轮 |
| PROPFIND 列目录 → GET | **1.408s** | **-21.9%** | 0/3 轮 |
| HEAD 探测 → GET | **1.196s** | **-33.7%** | 0/3 轮 |

小文件（单片，2KB）同样有效：1.012s → 0.543s（**-46%**）。
> 复现：`python3 warmup_test.py`，报告见 `WARMUP_REPORT.md`。

---

## 快速开始

### 1. 准备 Telegram
1. 找 [@BotFather](https://t.me/BotFather) 建一个 bot，拿到 **BOT_TOKEN**。
2. 建一个**频道或群组**，把 bot 加为**管理员**（否则发不进去）。
3. 拿到频道的 **CHAT_ID**：公开频道用 `@channelname`，私有频道/群组用 `-100xxxx`（可转发一条消息给 [@JsonViewBot](https://t.me/JsonViewBot) 查 `chat.id`）。

> 若服务器在国内无法直接连 `api.telegram.org`，设置 `TG_API_BASE` 指向自建代理（见下方）。

### 2. 配置并启动
```bash
cd telegram_webdav
cp .env.example .env        # 填入 TG_BOT_TOKEN / TG_CHAT_ID / DAV_USER / DAV_PASSWORD
python3 run.py              # 或 python3 -m server / python3 server.py
```
默认监听 `0.0.0.0:8080`，SQLite 数据库 `./telegram_webdav.db`。

### 3. 挂载
- **rclone**：`rclone config` 选 `webdav`，vendor=other，url=`http://host:8080/`，user/pass 填 `DAV_USER/DAV_PASSWORD`。
- **Windows 资源管理器**：地址栏输入 `\\host@8080\DavWWWRoot\` 或"映射网络驱动器"填 `http://host:8080/`。
- **macOS Finder**：`Finder ▸ 前往 ▸ 连接服务器` 填 `http://host:8080/`。

---

## 环境变量

| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `TG_BOT_TOKEN` | bot token（单 bot 模式） | 空 |
| `TG_CHAT_ID` | 频道/群组 chat_id（单 bot 模式） | 空 |
| `TG_BOT_POOLS` | 多 bot 池，JSON 数组 `[{"token","chatId","apiBase"(可选,字符串或数组),"proxyToken"(可选,字符串或数组)}]`；分摊 1 msg/s 流控。`apiBase`/`proxyToken` 缺省时回退全局变量；`apiBase` 写成数组即「该 bot 走多个 TG 代理」 | 空 |
| `TG_API_BASE` | 全局 Telegram API 代理基址（国内/被墙用），作为各 bot 未单独指定 `apiBase` 时的默认回退 | `https://api.telegram.org` |
| `TG_PROXY_TOKEN` | 全局代理鉴权令牌，以 `Authorization: Bearer` 头发出，作为各 bot 未单独指定 `proxyToken` 时的默认回退；官方 API 场景留空 | 空 |
| `TG_PROXY_POOLS` | 全局代理候选池（所有 bot 共享），JSON 数组：字符串数组 `["https://p1/tg","https://p2/tg"]` 或对象数组 `[{"apiBase":"https://p1/tg","proxyToken":"t1"},...]`；与每 bot 自带 `apiBase` 合并成候选列表，按序主备 + 失败自动切换（见下方「多个 TG 代理」）。**不会**顶掉 `TG_API_BASE`——主代理始终作为兜底候选保留 | 空 |
| `CHUNK_SIZE_MB` | 分片大小（≤20 即可走官方 Bot API；自建 Bot API Server 可到 2000） | `20` |
| `DB_PATH` | SQLite 文件路径 | `./telegram_webdav.db` |
| `DAV_USER` / `DAV_PASSWORD` | Basic 认证（建议必填） | 空（关闭认证） |
| `PORT` / `HOST` | 监听端口 / 地址 | `8080` / `0.0.0.0` |
| `TG_WEBHOOK_SECRET` | 频道入库 webhook 密钥（Telegram 以 `X-Telegram-Bot-Api-Secret-Token` 头发送） | 空 |
| `WEBDAV_IMPORT_DIR` | webhook 入库落盘目录 | `/telegram-import` |
| `TG_RATE_LIMIT` | 每 bot 发送最小间隔（秒），防 429 | `1.0` |
| `TG_SLOT_ROTATE` | 多 bot 池分片是否轮转分摊（`on`=各频道均匀承载；`off`=固定优先第一个，即主备模式） | `on` |
| `TG_UPLOAD_CONCURRENCY` | 上传分片并发线程数（`0`=自动，等于 bot 数量，受每 bot 1 msg/s 限流约束不超限） | `0`（=bot 数） |
| `TG_DOWNLOAD_CONCURRENCY` | 下载并发（保留兼容旧配置，**单文件下载已固定为单线程串行**，详见 webdav.py `_serve_file` 注释；多客户端/多文件并发仍在 `ThreadingHTTPServer` 层面自然并行） | `0` |
| `TG_STREAM_ALL_CHUNKS` | **播放关键**：是否让**所有分片**都边下边发。`off` = 旧行为（仅首片流式、其余整片缓冲 20MB 再一次性写出），播放会在每个分片边界干等一整片下载时间 | `on` |
| `TG_STREAM_BLOCK_KB` | 流式读块大小(KB)：从 Telegram 读到多少字节就写给客户端一次。越小→播放越平滑、起播/seek 首字节越快；纯大文件下载可调 1024 降开销 | `256` |
| `TG_WARMUP_PROPFIND` | 列目录时后台预取各文件首片的 `file_path`（省掉点开文件时 ~1s 的 getFile）。**不落库**——TG file_path 仅 1 小时有效 | `on` |
| `TG_WARMUP_HEAD` | HEAD 探测时预取该文件全部分片的 `file_path`（播放器探测后紧跟 GET） | `on` |
| `TG_WARMUP_MAX_FILES` | 单次列目录最多预热多少个文件，防止大目录把请求打爆代理 | `20` |
| `DAV_ROOT` | 把 WebDAV 根挂载到某子路径（默认 `/`），如 `/dav`；href 会自动带此前缀 | `/` |
| `DAV_KEEPALIVE` | 是否复用 TCP 连接（`on`/`off`）；个别客户端请求体长度数错导致错位时设 `off` | `on` |
| `DAV_BODY_TIMEOUT` | 请求体读取超时（秒），防止 `Content-Length` 虚高把线程拖死 | `300` |
| `DAV_IDLE_TIMEOUT` | keep-alive 空闲等待上限（秒），超时即回收空闲连接；自测可设小 | `30` |
| `DAV_LOG_IDLE_TIMEOUT` | 是否打印「连接空闲超时」日志。keep-alive 空闲回收是**正常行为**，默认不打（`off`）；想确认「日志里那些超时到底是啥」时临时设 `on` | `off` |

---

## 频道自动入库（Webhook）

把频道里别人发的文件自动变成 WebDAV 里的文件：

1. 设置 `TG_WEBHOOK_SECRET=xxx`。
2. 用任意 bot 管理工具 / curl 调 Telegram 设置 webhook：
   ```
   POST https://api.telegram.org/bot<TOKEN>/setWebhook
        ?url=https://你的域名/telegram/webhook
        &secret_token=xxx
   ```
   多 bot 池时每个 bot 各绑一个：`/telegram/webhook/0`、`/telegram/webhook/1`…
3. 频道里出现文件后，bot 收到消息，本服务提取 `file_id` 写进 `WEBDAV_IMPORT_DIR`，即可通过 WebDAV 下载。

> 下载 webhook 入库的文件时，用的就是当初接收它的那个 bot 的 token（slot 已记录）。

---

## 生产部署建议

前置 Nginx（关掉请求体缓冲，支持大文件流式上传）：
```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_request_buffering off;   # 关键：流式转发上传体
    proxy_buffering off;           # 关键：流式转发下载体
    client_max_body_size 0;        # 不限制上传大小
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
}
```
并发：本服务基于 `ThreadingHTTPServer`，单机中等并发足够；更高并发可套 gunicorn/uwsgi。

### 看日志：超时到底卡在哪个访问上

日志里出现超时时，会按「是不是真请求」分开处理，**不会再用一行没有主体的
`Request timed out: TimeoutError('timed out')` 糊过去**：

| 场景 | 日志 | 要不要管 |
| --- | --- | --- |
| **请求处理中超时**（读请求体/写响应卡住） | `请求超时(连接已关闭,需重新发起): 客户端=… 请求=PUT /path 卡在=读取请求体 已读请求体=192.0KB/共4.8MB(4%) 已耗时=4.0s 重试情况=上传分片重试2次 \| 该访问累计超时1次/客户端累计尝试3次` | 要：对着 method/path 排查客户端或网络 |
| **同一访问被重发** | `请求重传: … PUT /path —— 该访问此前已超时 1 次，这是第 2 次尝试（已成功的分片会按 SHA 去重复用，只补传失败片）` | 提示：重传有去重兜底，不会重传全量 |
| **keep-alive 空闲超时** | 默认**静默**（正常回收，不刷屏）；`DAV_LOG_IDLE_TIMEOUT=on` 时打 `连接空闲超时(正常回收,无需处理)` | 不用管 |

「重试情况」统计的是**这次请求内部的自动重试**：上传分片重试（`_UP_CHUNK_RETRY=3`，
429 按 `retry_after` 退避、其它指数退避）与下载分片瞬断重试（`_CHUNK_RETRY=3`）分别计数。
计数通过请求级观测对象累计——分片跑在线程池里，只有显式传下去才统计得到。

---

## Docker / Docker Hub

镜像已发布到 Docker Hub：**`totootao/telegram-webdav`**（`linux/amd64` + `linux/arm64` 多架构）。

### 一键拉起
```bash
docker run -d --name tg-webdav \
  -p 8080:8080 \
  -v tg-webdav-data:/data \
  -e TG_BOT_TOKEN=xxxx \
  -e TG_CHAT_ID=-100xxxx \
  -e DAV_USER=alice -e DAV_PASSWORD=secret \
  totootao/telegram-webdav:latest
```
- 数据（SQLite）落在容器 `/data`，建议挂卷持久化。
- 多 bot 池（每项可单独带 `apiBase`/`proxyToken`）：

```bash
docker run -d --name tg-webdav \
  -p 8080:8080 \
  -v tg-webdav-data:/data \
  -e DAV_USER=alice -e DAV_PASSWORD=secret \
  -e 'TG_BOT_POOLS=[{"token":"111:AAA","chatId":"-100xxx"},
                     {"token":"222:BBB","chatId":"-100yyy","apiBase":"https://proxy2/tg","proxyToken":"tok2"}]' \
  totootao/telegram-webdav:latest
```
  未在某 bot 写 `apiBase`/`proxyToken` 时，自动回退到全局 `TG_API_BASE`/`TG_PROXY_TOKEN`。

### 多个 TG 代理（主备 + 容灾切换）

手上不止一个 TG 代理时，把额外的配进 `TG_PROXY_POOLS`，某个代理挂了会自动退到下一个：

```bash
-e TG_API_BASE=https://tg.example.com/tg \
-e TG_PROXY_TOKEN=xxxxxxxx \
-e 'TG_PROXY_POOLS=[{"apiBase":"https://otterhub-tg-proxy-3uj.pages.dev/tg","proxyToken":"<该代理自己的令牌>"},
                     {"apiBase":"https://tg-proxy-b4t.pages.dev/tg","proxyToken":"<该代理自己的令牌>"}]'
```

池里可以放任意多个，按数组顺序依次做后备。`proxyToken` 省略则复用全局 `TG_PROXY_TOKEN`；
像 OtterHub 这类公开代理并不校验令牌，填不填都能通。

候选顺序与语义：

| 顺序 | 来源 | 说明 |
| --- | --- | --- |
| ① | bot 自带的 `apiBase`（`TG_BOT_POOLS` 里每项可配，支持数组） | 每 bot 独立指定 |
| ② | `TG_PROXY_POOLS` | 全局共享的额外候选 |
| ③ | `TG_API_BASE` + `TG_PROXY_TOKEN` | **始终追加**的兜底候选（与前面重复则去重） |

> ③ 这一条容易踩坑：早期版本写的是「只有列表为空才追加全局默认」，结果配了
> `TG_PROXY_POOLS` 之后 `TG_API_BASE` 里配的主代理被整个丢掉——想「多一个备用」，
> 实际变成「换掉主用」。现在修成始终追加，日志里可以看到
> `代理候选数=3 首候选=...`（池里 2 个 + 全局兜底 1 个）。

语义是**按序主备**，不是轮询分摊：正常请求永远走候选 ①，只有它出现网络错误、
超时或 5xx 才退到下一个；429（bot 级限流）和 4xx 业务错误不会换代理，
因为换代理没用（前者交给上层换 bot）。所以**把最快的那个放前面**就行。

启动时会为每个 bot 打印候选，照着核对最省事：

```
[tg]   槽位 0: chat_id=-1001549117195 代理候选数=3 首候选=https://otterhub-tg-proxy-3uj.pages.dev/tg
```

换代理前后的实测（2026-09-09，同一个 20MB 分片取前 8MB，各 2 轮）：

| 代理 | 轮 1 | 轮 2 | 说明 |
| --- | --- | --- | --- |
| `otterhub-tg-proxy-3uj.pages.dev` | 2.94 MB/s | 3.88 MB/s | 现主用 |
| `tg-proxy-b4t.pages.dev` | 2.94 MB/s | 2.67 MB/s | 本次新增，次选 |
| `tg.totootao.top` | 1.26 MB/s | 0.94 MB/s | 原主用，现兜底 |

三个代理都正确返回 `206 + Content-Range`，同区间取下的字节 MD5 完全一致
（`ed1c615f…`），可以放心互为备份。注意速度随时段波动很大，隔一阵子重测可能排名会变。

回归测试（含「首候选不可达时自动切换且数据一致」「全部候选不可达必须报错」）：
```bash
source <容器 TG_*/DAV_* 环境变量> && python3.11 proxy_failover_test.py   # 10/10
```

### 多 bot 池的分片分配

配了多个 bot 时，分片默认**轮转分摊**到各频道（`TG_SLOT_ROTATE=on`）：第 1 片→bot0、第 2 片→bot1……依次循环，
这样每个频道的承载量与每个 bot 的流控压力都被摊开，整体吞吐接近线性提升。

设 `TG_SLOT_ROTATE=off` 则退回**主备模式**：固定优先用第一个 bot，只有它失败/429 时才切到下一个。
适合有主备倾向（比如只有主频道做了备份）的场景。

> 注意：轮转只决定**起始槽位**，原有的失败/429 换槽重试逻辑不受影响——两者叠加，
> 不会因为轮转到某个恰好限流的 bot 就失败。
- 镜像构建与推送由仓库的 GitHub Actions 工作流（`.github/workflows/docker.yml`）自动完成：推送 `main` 或 `v*` 标签即触发，登录凭据来自仓库 Secrets `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN`。

### 自行构建
```bash
docker build -t telegram-webdav .
docker run -d -p 8080:8080 -v $PWD/data:/data telegram-webdav
```

---

## 自测

无需真实 bot，用本地"假 Telegram"端到端验证（分片 / Range / MOVE / COPY / DELETE / webhook）：
```bash
python3 selftest.py
```
预期输出 `结果: 28/28 通过`。

### 真实环境已验证

用真实 bot + 频道 + 自建 API 代理（`TG_API_BASE` + `TG_PROXY_TOKEN`）跑过端到端：

| 场景 | 结果 |
| --- | --- |
| 45MB 文件 PUT | 自动切成 3 片（20+20+5MB）落进频道，完整回读 MD5 一致 |
| HTTP Range（`bytes=1M-2M` 跨片、`bytes=44M-` 尾部） | 206 + 字节级一致 |
| 空文件 PUT / GET | 0 字节占位，回读 size=0 |
| MOVE / COPY / DELETE / PROPFIND Depth 1 | 正常；COPY 共享 `file_id`，45MB 副本 0.04 秒生成且不重复上传 |
| Basic 认证 | 无凭据 / 错误口令均 401 |
| 并发 4 路 PUT + 回读 | MD5 全部一致 |
| rclone 1.68（WebDAV 后端） | `copy` 整个目录、`check` 0 differences、回读 `diff -r` 无差异 |

> 注意：自建 API 代理若套了 Cloudflare，默认的 `Python-urllib/x.y` UA 会被拦（403 / error code 1010）。
> 本项目已把 UA 固定为 `TelegramWebDAV/1.0 (+python-urllib)`，无需额外配置。

---

## 客户端兼容性（AList / OpenList 挂载）

在 AList v3.64（OpenList 同源）里把本服务挂成 WebDav 存储即可正常读写：

| 操作 | AList 侧表现 | 备注 |
| --- | --- | --- |
| 挂载后首次列目录 | 正常 | 建议 WebDav 地址**以 `/` 结尾**（如 `http://ip:8080/`）；服务端现已对带/不带 `/` 都兼容 |
| 新建文件夹（单层 / 嵌套） | 正常 | `MKCOL`，父目录不存在时按规范返回 409 |
| 上传 / 下载文件 | 正常 | 走 `PUT` / `GET`，支持 Range |
| 删除文件 / 文件夹 | 正常 | 服务端递归删元数据；Telegram 侧的物理分片仍留频道 |

> **踩过的坑**：AList / OpenList 的 WebDav 驱动构造 PROPFIND / MKCOL / DELETE 的
> XML body 时，`Content-Length` 比真实 body 少算 1 字节（尾部那个 `\n` 没算进去）。
> 在 keep-alive 下，这个残留字节会被服务端当成下一个请求的起始行，于是吐出 Python
> 自带的 HTML 400 页，客户端报 `malformed HTTP status code "HTML>"`，表现为
> 「首次能连上、创建/删除全失败」。另一类常见坑是**挂载地址没以 `/` 结尾**，AList
> 会拼出畸形 URL 命中网关 HTML 页。
>
> 本项目的处理：
> - **路径与尾斜杠**：目录无论带不带 `/` 都能访问；PROPFIND 的 href 始终对集合补 `/`
>   （符合 RFC 4918，客户端相对路径才对）；`DAV_ROOT` 挂载时 href 自动带前缀。客户端侧
>   挂载地址仍以 `/` 结尾最稳（如 `http://ip:8080/`）。
> - **keep-alive 残包检测（非破坏性）**：服务每次处理完请求都读净请求体，并**非破坏性地**
>   探测「是否还有超出 `Content-Length` 的残留字节」——用 0 超时 peek，避免把 socket 读超时
>   变成莫名其妙的 500。探测到残留时，只要它**不像**一条新请求起始行（即确实是客户端数错长度
>   多发的那段字节），就直接**丢弃**，连接照样复用；一旦看起来像下一条请求（流水线）就停手。
>   没残留就放心复用连接，好客户端（rclone / Windows / macOS / cadaver）享受 keep-alive。
> - **回退开关**：若某客户端仍报 `HTML>`，设 `DAV_KEEPALIVE=off` 退回「每条连接只
>   服务一次」的保守模式即可，行为等价于早期版本。
> - **频道里的文件名 = 原文件名**：`PUT` 上传时把原始文件名（URL 最后一段）透传给
>   Telegram 的 `sendDocument` 的 `filename` 字段（参考 otterhub-server 的做法），
>   这样在频道里浏览时看到的就是 `report.pdf` 而不是千篇一律的 `part.bin`。单分片直接用
>   原文件名；多分片用「原文件名.partNN」既保留原名线索又能区分分片（下载仍按 `file_id`，
>   文件名仅影响频道展示，不影响内容）。

---

## 限制 / 注意

- **删除不可逆**：Telegram 不支持删除已发消息，DELETE 只删本地元数据，物理分片仍占频道空间（清频道即可彻底释放）。
- **file_id 与 bot 绑定**：换 bot / 重置 bot 会让旧 `file_id` 失效；多 bot 池请保持 bot 稳定。
- **单文件上限**：默认 20MB×N 分片，理论可很大；自建 Bot API Server 时把 `CHUNK_SIZE_MB` 调大更省请求数。
- 仅实现了 WebDAV 常用子集；`LOCK` 为兼容 Windows 的轻量实现（不强制锁冲突校验）。

---

## 文件结构

```
telegram_webdav/
├── config.py        # 环境变量配置
├── db.py            # SQLite 元数据层（文件树 / 锁）
├── tg.py            # Telegram 存储后端（上传/下载/webhook 解析）
├── webdav.py        # WebDAV 处理器 + App + 服务工厂
├── server.py        # 启动入口
├── run.py           # 便捷启动器
├── fake_telegram.py # 本地假 Telegram（自测用）
├── selftest.py      # 端到端自测
└── .env.example
```
