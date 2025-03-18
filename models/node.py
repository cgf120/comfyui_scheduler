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
                task_id = self.redis.get(self.config_manager.get("task_id_key") + prompt_id)
                task_info = self.redis.get(self.config_manager.get("task_info_key") + task_id)
                if task_info is None:
                    logger.warning(f"Task info not found for prompt {prompt_id}")
                    return
                task_info = json.loads(task_info)
                if task_info.get("logs") is None:
                    task_info["logs"] = []
                task_info["logs"].append(data)
                max_steps = task_info.get("max_steps", 0)
                pass_steps = task_info.get("pass_steps", 0)
                task_info["progress_node"] = None


                if msg_type == "execution_start":
                    if task_info is None:
                        logger.warning(f"Task info not found for prompt {prompt_id}")
                        return
                    task_info["status"] = "running"
                    task_info["start_time"] = data.get("timestamp")
                    logger.info(f"Prompt {prompt_id} 开始执行")

                elif msg_type == "executed" or  msg_type == "executing":
                    if task_info is None:
                        logger.warning(f"Task info not found for prompt {prompt_id}")
                        return
                    task_info["node"] = data.get("node")
                    pass_steps = pass_steps + 1
                    task_info['pass_steps'] = pass_steps
                    task_info['progress'] = pass_steps/max_steps
                    # 处理最后一个节点输出
                    if data.get("node") in task_info.get("output_nodes", []):
                        # todo 处理输出的下载地址信息
                        task_info['output'] = data.get("output")

                elif msg_type == "execution_cached":
                    if task_info is None:
                        logger.warning(f"Task info not found for prompt {prompt_id}")
                        return
                    task_info["node"] = data.get("nodes")
                    pass_steps = pass_steps + len(data.get("nodes"))
                    task_info['pass_steps'] = pass_steps
                    task_info['progress'] = pass_steps/max_steps
                elif msg_type == "progress":
                    if task_info is None:
                        logger.warning(f"Task info not found for prompt {prompt_id}")
                        return
                    task_info["progress_node"] = data.get("node")
                    current_value = data.get("value")
                    max_value = data.get("max")
                    task_info["progress_node"] = f'{current_value}/{max_value}'
                    if max_value == current_value:
                        pass_steps = pass_steps + 1
                        task_info['pass_steps'] = pass_steps
                        task_info['progress'] = pass_steps/max_steps
                elif msg_type == "execution_success":
                    if task_info is None:
                        logger.warning(f"Task info not found for prompt {prompt_id}")
                        return
                    task_info["status"] = "success"
                    task_info["end_time"] = data.get("timestamp")
                    logger.info(f"Prompt {prompt_id} 执行成功")
                elif msg_type == "execution_error":
                    if task_info is None:
                        logger.warning(f"Task info not found for prompt {prompt_id}")
                        return
                    task_info["status"] = "error"
                    task_info["node"] = data.get("node_id") #错误的时候给的是node_id
                    task_info["end_time"] = data.get("timestamp")
                    logger.error(f"Prompt {prompt_id} 执行出错: {data.get('error')}")
                elif msg_type == "execution_interrupted":
                    if task_info is None:
                        logger.warning(f"Task info not found for prompt {prompt_id}")
                        return
                    task_info["status"] = "interrupted"
                    task_info["node"] = data.get("node_id") #错误的时候给的是node_id
                    task_info["end_time"] = data.get("timestamp")
                    logger.warning(f"Prompt {prompt_id} 执行被中断")
                else:
                    logger.warning(f"未知消息类型: {msg_type}")
                self.redis.set(self.config_manager.get("task_info_key") + task_id, json.dumps(task_info))
            else:
                #其他消息不处理 目前有的系统监控信息 gpu/cpu使用等
                pass





            # 根据消息类型处理
            # if msg_type == 'status':
            #     # 处理状态更新消息
            #     data_status = data.get('data', {})
            #
            #
            #
            #
            #     prompt_id = data_status.get('prompt_id')
            #     if prompt_id is None:
            #         logger.warning(f"Prompt ID is missing in status message: {data}")
            #         return
            #     # 根据prompt_id获取task_id,然后根据task_id获取task_info
            #     task_id = await self.redis.get(self.config_manager.get("task_id_key") + prompt_id)
            #     task_info = await self.redis.get(self.config_manager.get("task_info_key") + task_id)
            #     if task_info is None:
            #         logger.warning(f"Task info not found for prompt {prompt_id}")
            #         return
            #     task_info = json.loads(task_info)
            #     if task_info.get("logs") is None:
            #         task_info["logs"] = []
            #     task_info["logs"].append(data_status)
            #     if 'progress' in data_status:
            #         # 处理进度信息
            #         progress = data_status.get('progress')
            #         total_steps = data_status.get('total_steps', 0)
            #         current_step = data_status.get('step', 0)
            #         logger.info(f"Prompt {prompt_id} 进度: {progress:.2f}% (步骤 {current_step}/{total_steps})")
            #         task_info["progress"] = progress # 进度
            #         task_info["node_id"] = current_step # 当前执行节点
            #         task_info["current_step"] = current_step # 当前执行步骤
            #         task_info["total_steps"] = total_steps # 总步骤
            #
            #     if 'execution_node' in data_status:
            #         # 处理当前执行节点信息
            #         node_id = data_status.get('execution_node')
            #         node_type = data_status.get('node_type', '未知')
            #         logger.info(f"Prompt {prompt_id} 正在执行节点: {node_id} (类型: {node_type})")
            #
            # elif msg_type == 'executing':
            #     # 处理节点执行信息
            #     data_exec = data.get('data', {})
            #     prompt_id = data_exec.get('prompt_id')
            #     node_id = data_exec.get('node', '未知')
            #     logger.info(f"Prompt {prompt_id} 开始执行节点: {node_id}")
            #
            # elif msg_type == 'execution_error':
            #     # 处理执行错误
            #     data_error = data.get('data', {})
            #     prompt_id = data_error.get('prompt_id')
            #     error_msg = data_error.get('error', '未知错误')
            #     node_id = data_error.get('node_id', '未知')
            #     logger.error(f"Prompt {prompt_id} 在节点 {node_id} 执行出错: {error_msg}")
            #
            # elif msg_type == 'execution_cached':
            #     # 处理缓存执行信息
            #     data_cached = data.get('data', {})
            #     prompt_id = data_cached.get('prompt_id')
            #     node_id = data_cached.get('node_id', '未知')
            #     logger.info(f"Prompt {prompt_id} 使用缓存执行节点: {node_id}")
            #
            # elif msg_type == 'progress':
            #     # 处理整体进度信息
            #     data_progress = data.get('data', {})
            #     prompt_id = data_progress.get('prompt_id')
            #     value = data_progress.get('value', 0)
            #     max_value = data_progress.get('max', 100)
            #     progress_percent = (value / max_value) * 100 if max_value > 0 else 0
            #     logger.info(f"Prompt {prompt_id} 总体进度: {progress_percent:.2f}%")
            #
            # elif msg_type == 'executed':
            #     # 处理完成信息
            #     data_completed = data.get('data', {})
            #     prompt_id = data_completed.get('prompt_id')
            #     logger.info(f"Prompt {prompt_id} 已完成执行")
            #
            #     # 处理输出文件信息
            #     if 'output' in data_completed:
            #         outputs = data_completed.get('output', {})
            #         output_files = []
            #
            #         # 遍历输出节点
            #         for node_id, node_output in outputs.items():
            #             # 检查是否有图像输出
            #             if 'images' in node_output:
            #                 for img in node_output['images']:
            #                     filename = img.get('filename')
            #                     if filename:
            #                         output_files.append(filename)
            #
            #             # 检查是否有视频输出
            #             if 'gifs' in node_output:
            #                 for vid in node_output['videos']:
            #                     filename = vid.get('filename')
            #                     if filename:
            #                         output_files.append(filename)
            #             # 音频输出
            #             if 'audio' in node_output:
            #                 for audio in node_output['audio']:
            #                     filename = audio.get('filename')
            #                     if filename:
            #                         output_files.append(filename)
            #
            #         if output_files:
            #             logger.info(f"Prompt {prompt_id} 输出文件: {', '.join(output_files)}")
            #         else:
            #             logger.info(f"Prompt {prompt_id} 没有输出文件")
            
            
        except json.JSONDecodeError as e:
            logger.error(f"解析节点 {self.node_id} WebSocket消息JSON时出错: {str(e)}")
        except Exception as e:
            logger.error(f"处理节点 {self.node_id} WebSocket消息时出错: {str(e)}")
    
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
            
            async with self.client_session.post(
                f"{self.api_url}/prompt", 
                json=prompt_data,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                result = await response.json()
                if response.status == 200:
                    logger.info(f"Prompt submitted to node {self.node_id}, prompt_id: {result.get('prompt_id')}, client_id: {self.client_id}")
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