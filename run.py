#!/usr/bin/env python3
"""便捷启动器：从项目根目录运行。

用法：
    cd telegram_webdav
    python3 run.py
"""
import sys
from server import main

if __name__ == "__main__":
    sys.exit(main())
