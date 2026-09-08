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
  把 `{file_id, slot, size, message_id}` 数组写进 SQLite。空文件用 1 字节占位并打标。
- **GET（下载）**：从 SQLite 读出分片索引，按请求的 `Range` 计算覆盖哪些分片，
  逐片向 Telegram 发 `Range` 请求并**流式拼接**返回（支持视频拖拽 / 断点续传）。
- **目录 / 移动 / 复制 / 删除**：纯 SQLite 文件树操作。COPY 共享同一批 `file_id`（物理不重复上传）；
  DELETE 只删元数据——Telegram 不支持删已发消息，物理分片仍留频道（与 otterhub 一致）。

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
| `DAV_ROOT` | 把 WebDAV 根挂载到某子路径（默认 `/`） | `/` |

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
