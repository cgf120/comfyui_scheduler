import time
import logging
import aiohttp
import subprocess
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("ComfyUI-Scheduler")

class ComfyNode:
    """表示一个ComfyUI节点实例"""
    def __init__(self, node_id: str, host: str, port: int, max_queue_size: int = 5, container_id: Optional[str] = None, server_id: Optional[str] = None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}"
        self.api_url = f"{self.url}/api"
        self.ws_url = f"ws://{host}:{port}/ws"
        self.status = "initializing"  # initializing, running, error
        self.queue_size = 0
        self.max_queue_size = max_queue_size
        self.last_heartbeat = time.time()
        self.process: Optional[subprocess.Popen] = None
        self.client_session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.is_local = (host == "localhost" or host == "127.0.0.1")
        self.container_id = container_id  # Docker容器ID
        self.server_id = server_id  # 所属服务器ID
    
    async def initialize(self):
        """初始化节点连接"""
        if self.client_session is None:
            self.client_session = aiohttp.ClientSession()
        
        try:
            # 检查节点是否在线
            async with self.client_session.get(f"{self.api_url}/system_stats", timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status == 200:
                    self.status = "running"
                    self.last_heartbeat = time.time()
                    logger.info(f"Node {self.node_id} ({self.url}) is online")
                    return True
                else:
                    self.status = "error"
                    logger.error(f"Node {self.node_id} returned status {response.status}")
                    return False
        except Exception as e:
            self.status = "error"
            logger.error(f"Failed to connect to node {self.node_id}: {str(e)}")
            return False
    
    async def connect_websocket(self):
        """连接到节点的WebSocket以接收实时更新"""
        if self.client_session is None:
            self.client_session = aiohttp.ClientSession()
        
        try:
            self.ws = await self.client_session.ws_connect(self.ws_url)
            logger.info(f"WebSocket connected to node {self.node_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect WebSocket to node {self.node_id}: {str(e)}")
            return False
    
    async def get_queue_info(self) -> Dict:
        """获取节点的队列信息"""
        if self.client_session is None:
            self.client_session = aiohttp.ClientSession()
        
        try:
            async with self.client_session.get(f"{self.api_url}/queue", timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status == 200:
                    data = await response.json()
                    self.last_heartbeat = time.time()
                    # 更新队列大小
                    running = len(data.get('queue_running', []))
                    pending = len(data.get('queue_pending', []))
                    self.queue_size = running + pending
                    return data
                else:
                    logger.warning(f"Failed to get queue info from node {self.node_id}, status: {response.status}")
                    return {}
        except Exception as e:
            logger.error(f"Error getting queue info from node {self.node_id}: {str(e)}")
            return {}
    
    async def submit_prompt(self, prompt_data: Dict) -> Tuple[bool, Dict]:
        """向节点提交提示"""
        if self.client_session is None:
            self.client_session = aiohttp.ClientSession()
        
        try:
            async with self.client_session.post(
                f"{self.api_url}/prompt", 
                json=prompt_data,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                result = await response.json()
                if response.status == 200:
                    logger.info(f"Prompt submitted to node {self.node_id}, prompt_id: {result.get('prompt_id')}")
                    return True, result
                else:
                    logger.warning(f"Failed to submit prompt to node {self.node_id}, status: {response.status}, error: {result}")
                    return False, result
        except Exception as e:
            logger.error(f"Error submitting prompt to node {self.node_id}: {str(e)}")
            return False, {"error": str(e)}
    
    async def close(self):
        """关闭节点连接"""
        if self.ws is not None:
            await self.ws.close()
            self.ws = None
        
        if self.client_session is not None:
            await self.client_session.close()
            self.client_session = None
        
        if self.is_local and self.process is not None:
            try:
                # 尝试优雅地关闭进程
                self.process.terminate()
                # 等待进程结束
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    # 如果超时，强制关闭
                    self.process.kill()
                logger.info(f"Local node {self.node_id} process terminated")
            except Exception as e:
                logger.error(f"Error terminating local node {self.node_id}: {str(e)}")
        
        # 注意：Docker容器的关闭由DockerServer处理，这里不需要额外操作
        
        self.status = "closed"
    
    def is_available(self) -> bool:
        """检查节点是否可用于新任务"""
        return (
            self.status == "running" and 
            self.queue_size < self.max_queue_size and
            (time.time() - self.last_heartbeat) < 30  # 30秒内有心跳
        )
    
    def is_idle(self) -> bool:
        """检查节点是否空闲（可以被缩容）"""
        return self.status == "running" and self.queue_size == 0