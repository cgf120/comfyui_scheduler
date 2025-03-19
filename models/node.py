import time
import logging
import aiohttp
import uuid
import asyncio
import subprocess
from typing import Dict, List, Optional, Tuple, Any
from utils.config_utils import ConfigManager
from utils.redis_utils import RedisManager

logger = logging.getLogger("ComfyUI-Scheduler")

class ComfyNode:
    """表示一个ComfyUI节点实例"""
    def __init__(self, redis:RedisManager,config_manager: ConfigManager,node_id: str, host: str, port: int, max_queue_size: int = 5, container_id: Optional[str] = None, server_id: Optional[str] = None):
        self.redis = redis
        self.config_manager = config_manager
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
        self.client_id = str(uuid.uuid4())  # 为每个节点生成唯一的client_id
        self.last_task_time = 0  # 上次任务时间
    
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

                    # 连接WebSocket
                    await self.connect_websocket()
                    logger.info(f"WebSocket连接已建立")

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
        """连接到ComfyUI节点的WebSocket并持续监听"""
        self.ws = None
        try:
            self.ws_session = aiohttp.ClientSession()
            # 在WebSocket URL中添加client_id参数
            ws_url = f"ws://{self.host}:{self.port}/ws?clientId={self.client_id}"
            self.ws = await self.ws_session.ws_connect(ws_url)
            logger.info(f"已连接到节点 {self.node_id} 的WebSocket，client_id: {self.client_id}")
            
            # 启动监听任务
            self.ws_task = asyncio.create_task(self._listen_websocket())
            return True
        except Exception as e:
            logger.error(f"连接节点 {self.node_id} 的WebSocket失败: {str(e)}")
            if self.ws_session:
                await self.ws_session.close()
            return False

    async def _listen_websocket(self):
        """监听WebSocket消息"""
        if self.ws is None:
            logger.warning(f"节点 {self.node_id} WebSocket未连接")
            return
        try:
            async for msg in self.ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    # 处理接收到的消息
                    await self._process_ws_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"节点 {self.node_id} WebSocket错误: {msg.data}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.info(f"节点 {self.node_id} WebSocket连接已关闭")
                    break
        except Exception as e:
            logger.error(f"监听节点 {self.node_id} WebSocket时出错: {str(e)}")
        finally:
            # 重新连接WebSocket
            if not self.ws.closed:
                await self.ws.close()
            
            # 如果节点仍然活跃，尝试重新连接
            if self.status != "offline":
                logger.info(f"尝试重新连接节点 {self.node_id} 的WebSocket")
                asyncio.create_task(self.connect_websocket())
    

    async def _process_ws_message(self, message):
        """处理WebSocket消息"""
        import json
        try:
            # 解析JSON消息
            message_data = json.loads(message)
            msg_type = message_data.get('type')
    
            # 记录完整消息（调试用）
            if msg_type != 'crystools.monitor':
                logger.info(f"节点 {self.node_id} WebSocket消息: {message}...")
    
            data = message_data.get('data', {})
            if data.get('prompt_id') is not None:
                prompt_id = data.get('prompt_id')
                # 使用异步任务处理WebSocket消息，避免阻塞主事件循环
                asyncio.create_task(self._handle_prompt_message(prompt_id, msg_type, data))
            else:
                #其他消息不处理 目前有的系统监控信息 gpu/cpu使用等
                pass
            
        except json.JSONDecodeError as e:
            logger.error(f"解析节点 {self.node_id} WebSocket消息JSON时出错: {str(e)}")
        except Exception as e:
            logger.error(f"处理节点 {self.node_id} WebSocket消息时出错: {str(e)}")

    async def _handle_prompt_message(self, prompt_id, msg_type, data):
        """处理与prompt相关的WebSocket消息"""
        try:
            # 添加超时控制的Redis操作
            task_id = await asyncio.wait_for(
                asyncio.to_thread(self.redis.get, self.config_manager.get("task_id_key") + prompt_id),
                timeout=2.0
            )
            
            if not task_id:
                logger.warning(f"Task ID not found for prompt {prompt_id}")
                return
                
            task_info_key = self.config_manager.get("task_info_key") + task_id
            task_info_str = await asyncio.wait_for(
                asyncio.to_thread(self.redis.get, task_info_key),
                timeout=2.0
            )
            
            if not task_info_str:
                logger.warning(f"Task info not found for prompt {prompt_id}, task_id: {task_id}")
                return
                
            import json
            task_info = json.loads(task_info_str)
            
            if task_info.get("logs") is None:
                task_info["logs"] = []
            task_info["logs"].append(data)
            max_steps = task_info.get("max_steps", 0)
            pass_steps = task_info.get("pass_steps", 0)
            task_info["progress_node"] = None
    
            # 根据消息类型更新任务信息
            if msg_type == "execution_start":
                task_info["status"] = "running"
                task_info["start_time"] = data.get("timestamp")
                logger.info(f"Prompt {prompt_id} 开始执行")
    
            elif msg_type == "executed" or msg_type == "executing":
                task_info["node"] = data.get("node")
                pass_steps = pass_steps + 1
                task_info['pass_steps'] = pass_steps
                task_info['progress'] = pass_steps/max_steps if max_steps > 0 else 0
                # 处理最后一个节点输出
                if data.get("node") in task_info.get("output_nodes", []):
                    task_info['output'] = data.get("output")
    
            elif msg_type == "execution_cached":
                task_info["node"] = data.get("nodes")
                nodes_count = len(data.get("nodes", []))
                pass_steps = pass_steps + nodes_count
                task_info['pass_steps'] = pass_steps
                task_info['progress'] = pass_steps/max_steps if max_steps > 0 else 0
                
            elif msg_type == "progress":
                task_info["progress_node"] = data.get("node")
                current_value = data.get("value", 0)
                max_value = data.get("max", 1)
                task_info["progress_node"] = f'{current_value}/{max_value}'
                if max_value > 0 and max_value == current_value:
                    pass_steps = pass_steps + 1
                    task_info['pass_steps'] = pass_steps
                    task_info['progress'] = pass_steps/max_steps if max_steps > 0 else 0
                    
            elif msg_type == "execution_success":
                task_info["status"] = "success"
                task_info["end_time"] = data.get("timestamp")
                logger.info(f"Prompt {prompt_id} 执行成功")
                
            elif msg_type == "execution_error":
                task_info["status"] = "error"
                task_info["node"] = data.get("node_id") #错误的时候给的是node_id
                task_info["end_time"] = data.get("timestamp")
                logger.error(f"Prompt {prompt_id} 执行出错: {data.get('error')}")
                
            elif msg_type == "execution_interrupted":
                task_info["status"] = "interrupted"
                task_info["node"] = data.get("node_id") #错误的时候给的是node_id
                task_info["end_time"] = data.get("timestamp")
                logger.warning(f"Prompt {prompt_id} 执行被中断")
                
            else:
                logger.warning(f"未知消息类型: {msg_type}")
                
            # 异步保存更新后的任务信息
            await asyncio.wait_for(
                asyncio.to_thread(self.redis.set, task_info_key, json.dumps(task_info)),
                timeout=2.0
            )
        except asyncio.TimeoutError:
            logger.error(f"处理prompt {prompt_id}消息时Redis操作超时")
        except Exception as e:
            logger.error(f"处理prompt {prompt_id}消息时出错: {str(e)}")
    
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
            # 修改prompt_data中的client_id为节点的client_id
            if "client_id" in prompt_data:
                original_client_id = prompt_data["client_id"]
                logger.info(f"替换任务client_id: {original_client_id} -> {self.client_id}")
            
            # 设置或替换client_id
            prompt_data["client_id"] = self.client_id
            
            # 使用更短的超时时间，避免长时间阻塞
            async with self.client_session.post(
                f"{self.api_url}/prompt", 
                json=prompt_data,
                timeout=aiohttp.ClientTimeout(total=15, connect=5)
            ) as response:
                result = await response.json()
                if response.status == 200:
                    logger.info(f"Prompt submitted to node {self.node_id}, prompt_id: {result.get('prompt_id')}, client_id: {self.client_id}")
                    return True, result
                else:
                    logger.warning(f"Failed to submit prompt to node {self.node_id}, status: {response.status}, error: {result}")
                    return False, result
        except asyncio.TimeoutError:
            logger.error(f"提交任务到节点 {self.node_id} 超时")
            return False, {"error": "Request timeout"}
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