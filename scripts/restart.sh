#!/bin/bash

PROCESS_NAME="bot.py"

echo "=== 开始部署流程 ==="

# 1. 获取进程 ID (PID)
# 使用 pgrep 查找包含 bot.py 的进程 ID
PIDS=$(pgrep -f "$PROCESS_NAME")

if [ -z "$PIDS" ]; then
    echo "未发现正在运行的 $PROCESS_NAME。"
else
    echo "发现旧进程，PID: $PIDS"
    
    if pgrep -f "$PROCESS_NAME" > /dev/null; then
        pgrep -f "$PROCESS_NAME" | xargs kill -9
        sleep 1 # 等待系统回收资源
        echo "已强制结束进程。"
    fi
fi

echo "--------------------------------"

# 5. 二次确认端口/进程是否残留
if pgrep -f "$PROCESS_NAME" > /dev/null; then
    echo "错误：无法停止旧进程，请手动检查！"
    ps -ef | grep "$PROCESS_NAME" | grep -v grep
    exit 1
fi

echo "正在启动新的 bot..."
# 使用 nohup 后台运行 (可选，如果你希望关掉终端 bot 还在跑)
# 或者直接运行：
uv run bot.py