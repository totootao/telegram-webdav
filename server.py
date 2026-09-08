"""服务入口：启动 TelegramWebDAV 服务。

运行方式（任选其一）：
    python3 -m telegram_webdav.server
    python3 telegram_webdav/server.py
    python3 run.py

配置全部走环境变量（见 .env.example）。
"""
import json
import sys

from webdav import make_server


def main():
    srv = make_server()
    cfg = srv.app.config
    print("=" * 48)
    print("TelegramWebDAV —— 用 Telegram 频道/群组当存储的纯 WebDAV 服务")
    print("=" * 48)
    print("配置:", json.dumps(cfg.summary(), ensure_ascii=False, indent=2))
    if not srv.app.backend:
        print("[警告] 未检测到 Telegram 配置（TG_BOT_TOKEN / TG_CHAT_ID 或 TG_BOT_POOLS）。")
        print("         PUT 上传会返回 503。请配置后再上传；PROPFIND/GET/目录操作可正常用。")
    print(f"监听: http://{cfg.host}:{cfg.port}")
    print(f"导入目录(webhook): {cfg.import_dir}")
    print("=" * 48)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n正在关闭...")
        srv.shutdown()


if __name__ == "__main__":
    sys.exit(main())
