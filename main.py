#!/usr/bin/env python3
import os
import sys
import asyncio
import logging
import argparse

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scheduler import ComfyUIScheduler
from utils.config_utils import ConfigManager
from utils.redis_utils import RedisManager

async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='ComfyUI Scheduler')
    parser.add_argument('--config', type=str, default='scheduler_config.json', help='配置文件路径')
    parser.add_argument('--port', type=int, default=8189, help='调度器监听端口')
    parser.add_argument('--gpu-device', type=int, help='本地节点使用的GPU设备ID')
    args = parser.parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("scheduler.log")
        ]
    )

    # 加载配置
    config_manager = ConfigManager(args.config)
    #初始化redis
    redis_manager  =RedisManager(config_manager.get('redis', {}))

    # 创建调度器
    scheduler = ComfyUIScheduler(config_manager,redis_manager)
    
    # 覆盖配置中的端口
    if args.port:
        scheduler.scheduler_port = args.port
    
    try:
        # 启动调度器
        asyncio.create_task(scheduler.start())
        
        # 保持运行
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down scheduler...")
    finally:
        # 停止调度器
        await scheduler.stop()

if __name__ == "__main__":
    # 检查依赖
    try:
        import aiohttp
        import psutil
        import docker
    except ImportError:
        print("请安装必要的依赖: pip install aiohttp psutil docker")
        sys.exit(1)
    
    # 运行主函数
    asyncio.run(main())