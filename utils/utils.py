import os
import sys
import logging
import psutil
import subprocess
from typing import Optional, List

logger = logging.getLogger("ComfyUI-Scheduler")

def check_port_available(port: int) -> bool:
    """检查端口是否可用"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.cmdline()
            if len(cmdline) > 1 and 'python' in cmdline[0].lower():
                for arg in cmdline:
                    if f"--port={port}" in arg:
                        return False
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return True

def find_available_port(start_port: int, end_port: int = 65535) -> Optional[int]:
    """查找可用端口"""
    for port in range(start_port, end_port + 1):
        if check_port_available(port):
            return port
    return None

def start_process(cmd: List[str], cwd: str = '') -> Optional[subprocess.Popen]:
    """启动子进程"""
    try:
        process = subprocess.Popen(
            cmd,
            cwd=cwd or os.path.dirname(os.path.abspath(__file__)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return process
    except Exception as e:
        logger.error(f"Error starting process: {str(e)}")
        return None


def convert_to_two_decimal(obj):
    if isinstance(obj, dict):  # 如果是字典
        for key, value in obj.items():
            obj[key] = convert_to_two_decimal(value)
    elif isinstance(obj, list):  # 如果是列表
        for i, value in enumerate(obj):
            obj[i] = convert_to_two_decimal(value)
    elif isinstance(obj, float):  # 如果是浮点数
        return round(obj, 2)
    return obj