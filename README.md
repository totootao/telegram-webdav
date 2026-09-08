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
| `TG_BOT_POOLS` | 多 bot 池，JSON 数组 `[{"token","chatId"}]`；分摊 1 msg/s 流控 | 空 |
| `TG_API_BASE` | Telegram API 代理基址（国内/被墙用） | `https://api.telegram.org` |
| `TG_PROXY_TOKEN` | 自建代理的鉴权令牌，以 `Authorization: Bearer` 头发出；官方 API 场景留空 | 空 |
| `CHUNK_SIZE_MB` | 分片大小（≤20 即可走官方 Bot API；自建 Bot API Server 可到 2000） | `20` |
| `DB_PATH` | SQLite 文件路径 | `./telegram_webdav.db` |
| `DAV_USER` / `DAV_PASSWORD` | Basic 认证（建议必填） | 空（关闭认证） |
| `PORT` / `HOST` | 监听端口 / 地址 | `8080` / `0.0.0.0` |
| `TG_WEBHOOK_SECRET` | 频道入库 webhook 密钥（Telegram 以 `X-Telegram-Bot-Api-Secret-Token` 头发送） | 空 |
| `WEBDAV_IMPORT_DIR` | webhook 入库落盘目录 | `/telegram-import` |
| `TG_RATE_LIMIT` | 每 bot 发送最小间隔（秒），防 429 | `1.0` |
| `DAV_ROOT` | 把 WebDAV 根挂载到某子路径（默认 `/`），如 `/dav`；href 会自动带此前缀 | `/` |
| `DAV_KEEPALIVE` | 是否复用 TCP 连接（`on`/`off`）；个别客户端请求体长度数错导致错位时设 `off` | `on` |
| `DAV_BODY_TIMEOUT` | 请求体读取超时（秒），防止 `Content-Length` 虚高把线程拖死 | `300` |
| `DAV_IDLE_TIMEOUT` | keep-alive 空闲等待上限（秒），超时即回收空闲连接；自测可设小 | `30` |

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
- 多 bot 池：`TG_BOT_POOLS='[{"token":"...","chatId":"-100..."}]'`。
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
