"""配置层：从环境变量读取 Telegram 存储与 WebDAV 服务配置。

参考 otterhub-server 的 .env.example：
  - TG_BOT_TOKEN / TG_CHAT_ID          单 bot 模式（频道/群组 chat_id，可为 @channel 或 -100xxxx）
  - TG_BOT_POOLS                       多 bot 池（JSON 数组：[{"token","chatId",
                                        "apiBase"(可选),"proxyToken"(可选)}]），分摊 1 msg/s 流控
  - TG_API_BASE                        自建 Telegram API 代理基址（国内/被墙环境），可选；
                                        作为 TG_BOT_POOLS 各槽位缺省 apiBase 的全局回退
  - TG_PROXY_TOKEN                     自建代理的鉴权令牌，作为各槽位缺省 proxyToken 的全局回退；可选
  - CHUNK_SIZE_MB                      分片大小，默认 20（Telegram Bot API 官方上传上限 20MB / 50MB）
  - DAV_USER / DAV_PASSWORD            WebDAV Basic 认证
  - DB_PATH                            SQLite 数据库路径

设计要点：apiBase / proxyToken 既可以作为全局环境变量（TG_API_BASE / TG_PROXY_TOKEN）统一设置，
也可以在每个 TG_BOT_POOLS 槽位里单独覆盖（键名 apiBase / proxyToken）。槽位级优先于全局级，
这样同一个服务里能让不同 bot 走不同代理。
"""
import os
import json


def _load_dotenv(path=".env"):
    """零依赖地加载 .env（KEY=VALUE，忽略 # 注释与空行）。已存在的环境变量不覆盖。"""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if not k:
                    continue
                v = v.strip("'\"")  # 去掉可能的引号
                os.environ.setdefault(k, v)
    except Exception:
        pass


_load_dotenv()


def _load_pools():
    """解析 TG_BOT_POOLS（JSON 数组）或回退到单 bot（TG_BOT_TOKEN / TG_CHAT_ID）。

    每个槽位支持：
        {"token", "chatId"/"chat_id",
         "apiBase"/"api_base"(可选), "proxyToken"/"proxy_token"(可选)}
    - apiBase / proxyToken 缺省时回退到全局 TG_API_BASE / TG_PROXY_TOKEN。
    - 这样同一个服务里的不同 bot 可各自走不同的 Telegram API 代理 / 鉴权令牌。
    """
    raw = os.environ.get("TG_BOT_POOLS")
    pools = []
    if raw and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                for it in parsed:
                    if not isinstance(it, dict):
                        continue
                    token = str(it.get("token", "")).strip()
                    chat_id = str(it.get("chatId") or it.get("chat_id") or "").strip()
                    if token and chat_id:
                        slot = {"token": token, "chat_id": chat_id}
                        api_base = str(
                            it.get("apiBase") or it.get("api_base") or ""
                        ).strip()
                        if api_base:
                            slot["api_base"] = api_base.rstrip("/")
                        proxy_token = str(
                            it.get("proxyToken") or it.get("proxy_token") or ""
                        ).strip()
                        if proxy_token:
                            slot["proxy_token"] = proxy_token
                        pools.append(slot)
        except Exception:
            # 简化格式：token|chatId,token|chatId（不支持 per-slot 的 apiBase/proxyToken）
            for part in raw.split(","):
                seg = part.strip()
                if not seg:
                    continue
                a, _, b = seg.partition("|")
                if a.strip() and b.strip():
                    pools.append({"token": a.strip(), "chat_id": b.strip()})
    if not pools:
        token = os.environ.get("TG_BOT_TOKEN")
        chat_id = os.environ.get("TG_CHAT_ID")
        if token and chat_id:
            pools.append({"token": token.strip(), "chat_id": chat_id.strip()})
    return pools


class Config:
    def __init__(self):
        self.db_path = os.environ.get("DB_PATH", "./telegram_webdav.db")
        self.chunk_size = int(os.environ.get("CHUNK_SIZE_MB", "20")) * 1024 * 1024
        # 全局默认 API 基址：作为 TG_BOT_POOLS 各槽位未单独指定 apiBase 时的回退。
        self.api_base = os.environ.get(
            "TG_API_BASE", "https://api.telegram.org"
        ).rstrip("/")
        # 自建 Telegram API 代理（如 tg.<domain>/tg）若额外要求认证，用此令牌
        # 以 Authorization: Bearer <token> 头发送；官方 api.telegram.org 下不需要，留空即可。
        # 同样作为 TG_BOT_POOLS 各槽位未单独指定 proxyToken 时的回退。
        self.proxy_token = (os.environ.get("TG_PROXY_TOKEN") or "").strip()
        self.slots = _load_pools()
        self.auth_user = os.environ.get("DAV_USER")
        self.auth_password = os.environ.get("DAV_PASSWORD")
        self.host = os.environ.get("HOST", "0.0.0.0")
        self.port = int(os.environ.get("PORT", "8080"))
        self.webhook_secret = os.environ.get("TG_WEBHOOK_SECRET")
        self.import_dir = (os.environ.get("WEBDAV_IMPORT_DIR", "/telegram-import") or "/telegram-import").rstrip("/") or "/telegram-import"
        self.rate_limit = float(os.environ.get("TG_RATE_LIMIT", "1.0"))
        # 多 bot 池的分片分配策略：on=轮转分摊（默认，各频道均匀承载）；
        # off=固定优先用第一个槽位（主备模式，只有失败/429 才切换）。
        self.slot_rotate = (
            os.environ.get("TG_SLOT_ROTATE", "on").strip().lower()
            not in ("0", "off", "false", "no")
        )
        self.root_path = (os.environ.get("DAV_ROOT", "/") or "/").rstrip("/") or "/"
        # keep-alive：默认开启（好客户端复用连接）；若某客户端仍报
        # `malformed HTTP status code "HTML>"`，设 off 退回「每条连接只服务一次」。
        self.keepalive = (
            os.environ.get("DAV_KEEPALIVE", "on").strip().lower()
            not in ("0", "off", "false", "no")
        )
        # 请求体读取超时（秒）：Content-Length 虚高时不会把线程拖死。
        self.body_timeout = float(os.environ.get("DAV_BODY_TIMEOUT", "300"))
        # keep-alive 空闲等待上限（秒）：超过则关闭空闲连接回收线程。
        self.idle_timeout = float(os.environ.get("DAV_IDLE_TIMEOUT", "30"))

    @property
    def auth_enabled(self):
        return bool(self.auth_user)

    def summary(self):
        return {
            "db_path": self.db_path,
            "chunk_size_mb": self.chunk_size // (1024 * 1024),
            "api_base": self.api_base,
            "proxy_auth": "on" if self.proxy_token else "off",
            "bot_slots": len(self.slots),
            "auth": "on" if self.auth_enabled else "off",
            "import_dir": self.import_dir,
            "rate_limit_s": self.rate_limit,
            "slot_rotate": "on" if self.slot_rotate else "off",
            "keepalive": "on" if self.keepalive else "off",
            "body_timeout_s": self.body_timeout,
            "idle_timeout_s": self.idle_timeout,
        }


config = Config()
