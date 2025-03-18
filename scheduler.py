import sys
import json
import time
import logging
import asyncio
import threading
import uuid
from typing import Dict,  Optional, Tuple, Any
from aiohttp import web

from models.node import ComfyNode
from models.docker_server import DockerServer
from api import setup_routes
from models.workflow import ComfyWorkflow
from utils.utils import check_port_available, find_available_port, start_process

from utils.redis_utils import RedisManager
from utils.config_utils import ConfigManager

logger = logging.getLogger("ComfyUI-Scheduler")


def create_docker_server(server_id: str, host: str, port: int = 2375,
                         max_containers: int = 8, gpu_ids=None) -> DockerServer:
    """创建Docker服务器实例"""
    if gpu_ids is None:
        gpu_ids = list(range(8))
    return DockerServer(
        server_id=server_id,
        host=host,
        port=port,
        max_containers=max_containers,
        gpu_ids=gpu_ids
    )


class ComfyUIScheduler:
    """ComfyUI调度器，负责管理节点和分发任务"""
    def __init__(self, config_manager: ConfigManager,redis_manager: RedisManager):
        self.config_manager = config_manager
        self.nodes: Dict[str, ComfyNode] = {}
        self.docker_servers: Dict[str, DockerServer] = {}
        self.task_queue = asyncio.Queue()
        self.running = False
        self.lock = threading.Lock()
        self.min_nodes = self.config_manager.get("min_nodes", 1)
        self.max_nodes = self.config_manager.get("max_nodes", 5)
        self.node_port_start = self.config_manager.get("node_port_start", 8188)
        self.scheduler_port = self.config_manager.get("scheduler_port", 8189)
        self.app = None
        self.runner = None
        self.site = None
        # 初始化Redis连接
        self.redis = redis_manager


    
    def get_workflow(self, workflow_id: str) -> Optional[ComfyWorkflow]:
        """获取工作流"""
        data =  self.redis.get(self.config_manager.get("workflow_key_prefix") + workflow_id)

        if data:
            return ComfyWorkflow(**json.loads(data))
        return None
    
    
    async def submit_workflow(self, 
                             workflow_id: str,
                              input_values=None,
                             client_id: str = '') -> Tuple[bool, str, Dict[str, Any]]:
        """
        提交工作流任务
        
        Args:
            workflow_id: 工作流ID
            input_values: 输入参数值，格式为 {node_id: {param_name: value}}
            client_id: 客户端ID，如果不提供则自动生成
            
        Returns:
            (成功标志, 任务ID, 结果信息)
        """
        # 获取工作流
        if input_values is None:
            input_values = {}
        workflow = self.get_workflow(workflow_id)
        if not workflow:
            return False, "", {"error": f"Workflow not found: {workflow_id}"}
        
        # 准备提交数据
        prompt_data = workflow.prepare_submission(input_values)
        
        # 生成任务ID
        task_id = str(uuid.uuid4())
        
        # 设置client_id
        if client_id:
            prompt_data["client_id"] = client_id

        workflow['prompt'] = prompt_data
        # 将任务加入队列
        await self.task_queue.put((task_id, workflow))
        
        return True, task_id, {"message": f"Task {task_id} submitted successfully"}

    
    def create_node(self, node_id: str, host: str, port: int, max_queue_size: int = 5,
                   container_id: Optional[str] = None, server_id: Optional[str] = None) -> ComfyNode:
        """创建节点实例"""
        return ComfyNode(
            config_manager=self.config_manager,
            redis=self.redis,
            node_id=node_id,
            host=host,
            port=port,
            max_queue_size=max_queue_size,
            container_id=container_id if container_id else "",
            server_id=server_id if server_id else ""
        )

    async def start(self):
        """启动调度器"""
        self.running = True
        
        # 初始化Docker服务器
        for server_info in self.config_manager.get("docker_servers", []):
            server_id = str(uuid.uuid4())
            server = create_docker_server(
                server_id=server_id,
                host=server_info["host"],
                port=server_info.get("port", 2375),
                max_containers=server_info.get("max_containers", 8),
                gpu_ids=server_info.get("gpu_ids", list(range(8)))
            )
            if await server.initialize():
                self.docker_servers[server_id] = server
        
        # 初始化远程节点
        for node_info in self.config_manager.get("remote_nodes", []):
            node_id = str(uuid.uuid4())
            node = self.create_node(
                node_id,
                node_info["host"],
                node_info["port"],
                node_info.get("max_queue_size", 5)
            )
            if await node.initialize():
                self.nodes[node_id] = node
                # 启动监控任务
                asyncio.create_task(self._monitor_node(node))
        
        # 确保至少有最小数量的节点
        await self._ensure_min_nodes()
        
        # 启动Web服务器
        await self._start_web_server()
        
        # 启动任务处理循环
        asyncio.create_task(self._process_tasks())
        
        # 启动节点监控循环
        asyncio.create_task(self._monitor_nodes())
        
        logger.info(f"ComfyUI Scheduler started on port {self.scheduler_port}")

    async def stop(self):
        """停止调度器"""
        self.running = False
        
        # 关闭所有节点
        for node_id, node in list(self.nodes.items()):
            await node.close()
        
        # 关闭所有Docker服务器
        for server_id, server in list(self.docker_servers.items()):
            server.close()
        
        # 关闭Web服务器
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        
        logger.info("ComfyUI Scheduler stopped")
    
    async def _start_web_server(self):
        """启动Web服务器提供API"""
        self.app = web.Application()
        setup_routes(self.app, self)
        
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '0.0.0.0', self.scheduler_port)
        await self.site.start()
        
        logger.info(f"Web server started on port {self.scheduler_port}")
    
    async def _ensure_min_nodes(self):
        """确保至少有最小数量的节点运行"""
        active_nodes = len([n for n in self.nodes.values() if n.status == "running"])
        
        if active_nodes < self.min_nodes:
            nodes_to_start = self.min_nodes - active_nodes
            logging.info(f"Starting {nodes_to_start} new nodes to meet minimum requirement")
            
            # 优先使用Docker服务器启动节点
            docker_servers_available = [s for s in self.docker_servers.values() 
                                       if s.status == "running" and len(s.available_gpu_ids) > 0]
            
            # 创建并行启动任务
            start_tasks = []
            for i in range(nodes_to_start):
                # 计算当前节点应该使用的端口
                port = self.node_port_start + len(self.nodes) + i
                
                # 创建启动任务
                if docker_servers_available:
                    # 选择可用GPU最多的服务器
                    server = max(docker_servers_available, key=lambda s: len(s.available_gpu_ids))
                    # 添加Docker节点启动任务
                    start_tasks.append(self._start_docker_node(server, port))
                else:
                    # 添加本地节点启动任务
                    start_tasks.append(self._start_local_node(port))
            
            # 并行执行所有启动任务
            if start_tasks:
                nodes = await asyncio.gather(*start_tasks)
                
                # 处理启动结果，添加成功启动的节点
                for node in nodes:
                    if node:
                        self.nodes[node.node_id] = node
                        asyncio.create_task(self._monitor_node(node))
    
    async def _start_docker_node(self, server: DockerServer, port: int) -> Optional[ComfyNode]:
        """在Docker服务器上启动ComfyUI节点"""
        try:
            container_id, gpu_id = await server.start_comfyui_container(port)
            if not container_id:
                # 如果无法在Docker上启动，尝试本地启动
                logging.warning(f"Failed to start Docker container on server {server.server_id}, trying local")
                return await self._start_local_node(port)
            
            # 创建节点
            node_id = str(uuid.uuid4())
            node = self.create_node(
                node_id=node_id,
                host=server.host,
                port=port,
                max_queue_size=5,
                container_id=container_id,
                server_id=server.server_id
            )
            
            # 等待节点初始化
            max_retries = 300
            for j in range(max_retries):
                if await node.initialize():
                    logging.info(f"Docker node {node_id} started on server {server.server_id} with GPU {gpu_id}")
                    return node
                await asyncio.sleep(5)
            
            # 如果无法初始化，停止容器
            logging.warning(f"Failed to initialize Docker node on server {server.server_id}, stopping container")
            await server.stop_container(container_id)
            
            # 尝试本地启动
            return await self._start_local_node(port)
        except Exception as e:
            logging.error(f"Error starting Docker node: {str(e)}")
            return None
    
    async def _start_local_node(self, port: int) -> Optional[ComfyNode]:
        """启动本地ComfyUI节点"""
        try:
            # 检查端口是否已被占用
            if not check_port_available(port):
                logging.warning(f"Port {port} is already in use")
                port = find_available_port(self.config_manager.get("node_port_start", 8188))
            
            # 构建启动命令
            cmd = [sys.executable, "main.py", f"--port={port}", "--listen=0.0.0.0"]
            
            # 添加其他必要的参数
            import argparse
            parser = argparse.ArgumentParser()
            parser.add_argument('--gpu-device', type=int, help='GPU设备ID')
            args, _ = parser.parse_known_args()
            
            if args.gpu_device is not None:
                cmd.append(f"--gpu-device={args.gpu_device}")
            
            # 启动进程
            process = start_process(cmd)
            if not process:
                return None
            
            # 创建节点对象
            node_id = str(uuid.uuid4())
            node = self.create_node(
                node_id=node_id,
                host="localhost",
                port=port,
                max_queue_size=5
            )
            node.process = process
            
            # 等待节点启动
            max_retries = 300
            for i in range(max_retries):
                if await node.initialize():
                    logging.info(f"Local node {node_id} started on port {port}")
                    return node
                await asyncio.sleep(5)
            
            # 如果无法启动，终止进程
            process.terminate()
            logging.error(f"Failed to start local node on port {port} after {max_retries} retries")
            return None
        except Exception as e:
            logging.error(f"Error starting local node: {str(e)}")
            return None
    
    async def _monitor_nodes(self):
        """监控所有节点的状态"""
        while self.running:
            try:
                # 检查节点状态
                for node_id, node in list(self.nodes.items()):
                    if node.status == "running":
                        # 获取队列信息
                        await node.get_queue_info()
                        
                        # 检查心跳
                        if (time.time() - node.last_heartbeat) > 30:
                            logger.warning(f"Node {node_id} heartbeat timeout")
                            node.status = "error"
                    
                    # 如果节点出错，尝试重新初始化
                    if node.status == "error":
                        if await node.initialize():
                            node.status = "running"
                            logger.info(f"Node {node_id} recovered")
                
                # 自动扩缩容
                if self.config_manager.get("auto_scaling", True):
                    await self._auto_scale()
                
                # 等待下一次检查
                await asyncio.sleep(self.config_manager.get("node_check_interval", 10))
            except Exception as e:
                logger.error(f"Error monitoring nodes: {str(e)}")
                await asyncio.sleep(5)
    
    async def _auto_scale(self):
        """根据队列长度自动扩缩容节点"""
        active_nodes = [n for n in self.nodes.values() if n.status == "running"]
        
        if not active_nodes:
            return
        
        # 计算平均队列长度
        total_queue_size = sum(n.queue_size for n in active_nodes)
        avg_queue_size = total_queue_size / len(active_nodes)
        
        # 扩容：如果平均队列长度超过阈值且节点数小于最大值
        if avg_queue_size > self.config_manager.get("scale_up_threshold", 3) and len(active_nodes) < self.max_nodes:
            logging.info(f"Auto-scaling: Starting new node (avg queue size: {avg_queue_size:.2f})")
            
            # 优先使用Docker服务器
            docker_servers_available = [s for s in self.docker_servers.values() 
                                       if s.status == "running" and len(s.available_gpu_ids) > 0]
            
            if docker_servers_available:
                # 选择可用GPU最多的服务器
                server = max(docker_servers_available, key=lambda s: len(s.available_gpu_ids))
                port = self.node_port_start + len(self.nodes)
                
                container_id, gpu_id = await server.start_comfyui_container(port)
                if container_id:
                    # 创建节点
                    node_id = str(uuid.uuid4())
                    node = self.create_node(
                        node_id=node_id,
                        host=server.host,
                        port=port,
                        max_queue_size=5,
                        container_id=container_id,
                        server_id=server.server_id
                    )
                    
                    # 等待节点初始化
                    max_retries = 30
                    for i in range(max_retries):
                        if await node.initialize():
                            self.nodes[node_id] = node
                            asyncio.create_task(self._monitor_node(node))
                            logging.info(f"Auto-scaling: Docker node {node_id} started on server {server.server_id} with GPU {gpu_id}")
                            return
                        await asyncio.sleep(1)
                    
                    # 如果无法初始化，停止容器
                    await server.stop_container(container_id)
            
            # 如果无法在Docker上启动或没有可用的Docker服务器，尝试本地启动
            port = self.node_port_start + len(self.nodes)
            node = await self._start_local_node(port)
            if node:
                self.nodes[node.node_id] = node
                asyncio.create_task(self._monitor_node(node))
        
        # 缩容：如果有空闲节点且节点数大于最小值
        elif avg_queue_size < self.config_manager.get("scale_down_threshold", 0) and len(active_nodes) > self.min_nodes:
            # 找到空闲节点
            idle_nodes = [n for n in active_nodes if n.is_idle()]
            if idle_nodes:
                # 优先移除Docker节点，因为它们占用了GPU资源
                docker_nodes = [n for n in idle_nodes if n.container_id and n.server_id]
                if docker_nodes:
                    # 选择最久未使用的Docker节点
                    node_to_remove = min(docker_nodes, key=lambda n: n.last_heartbeat)
                else:
                    # 如果没有Docker节点，选择最久未使用的普通节点
                    node_to_remove = min(idle_nodes, key=lambda n: n.last_heartbeat)
                
                logging.info(f"Auto-scaling: Removing idle node {node_to_remove.node_id}")
                
                # 如果是Docker节点，停止容器
                if node_to_remove.container_id and node_to_remove.server_id and node_to_remove.server_id in self.docker_servers:
                    server = self.docker_servers[node_to_remove.server_id]
                    await server.stop_container(node_to_remove.container_id)
                
                # 关闭节点
                await node_to_remove.close()
                del self.nodes[node_to_remove.node_id]
    
    async def _monitor_node(self, node: ComfyNode):
        """监控单个节点的状态"""
        while self.running and node.node_id in self.nodes:
            try:
                # 获取队列信息
                await node.get_queue_info()
                
                # 如果节点出错，尝试重新初始化
                if node.status == "error":
                    if await node.initialize():
                        node.status = "running"
                        logger.info(f"Node {node.node_id} recovered")
                
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error monitoring node {node.node_id}: {str(e)}")
                node.status = "error"
                await asyncio.sleep(5)
    
    async def _process_tasks(self):
        """处理任务队列"""
        while self.running:
            try:
                # 获取任务
                task_id, task_data = await self.task_queue.get()
                
                # 选择最佳节点
                node = self._select_best_node()
                
                if node is None:
                    # 如果没有可用节点，等待一段时间后重新入队
                    logging.warning(f"No available nodes for task {task_id}, requeuing")
                    await asyncio.sleep(5)
                    await self.task_queue.put((task_id, task_data))
                    continue
                # prompt_datab本身包含工作流信息和prompt数据
                prompt_data = task_data.get("prompt", {})
                # 记录原始client_id（如果存在）
                original_client_id = prompt_data.get("client_id", "未指定")
                logger.info(f"任务 {task_id} 从client_id: {original_client_id} 分配到节点 {node.node_id} (client_id: {node.client_id})")
                
                # 提交任务到节点（节点内部会替换client_id）
                success, result = await node.submit_prompt(prompt_data)
                
                if not success:
                    # 如果提交失败，重新入队
                    logging.warning(f"Failed to submit task {task_id} to node {node.node_id}, requeuing")
                    await asyncio.sleep(1)
                    await self.task_queue.put((task_id, task_data))
                else:
                    node.last_task_time = time.time()
                    prompt_id = result.get("prompt_id", "未知")
                    key = self.config_manager.get("task_info_key", "comfyui:task_info:") + task_id
                    task_data["node_id"] = node.node_id
                    task_data["prompt_id"] = prompt_id
                    task_data["node"] = None
                    task_data["status"] = "submitted"
                    task_data["prompt"] = prompt_data
                    task_data["max_steps"] = len(prompt_data.get("prompt", 0))
                    task_data["pass_steps"] = 0 # 已完成的步骤
                    task_data["progress"] = 0.00 # 进度
                    # 把处理信息推送到redis
                    self.redis.set(key, json.dumps(task_data))
                    logger.info(f"任务 {task_id} 提交成功，prompt_id: {prompt_id}")
                    # 保存task_id与prompt_id的映射
                    self.redis.set(self.config_manager.get("task_id_key", "comfyui:task_id:") + prompt_id,
                                         task_id)
                
                self.task_queue.task_done()
            except Exception as e:
                logger.error(f"Error processing task: {str(e)}")
                await asyncio.sleep(1)
    
    def _select_best_node(self) -> Optional[ComfyNode]:
        """选择最佳节点处理任务"""
        available_nodes = [n for n in self.nodes.values() if n.is_available()]
        
        if not available_nodes:
            return None
        
        # 按队列长度分组
        nodes_by_queue_size = {}
        for node in available_nodes:
            if node.queue_size not in nodes_by_queue_size:
                nodes_by_queue_size[node.queue_size] = []
            nodes_by_queue_size[node.queue_size].append(node)
        
        # 获取最小队列长度
        min_queue_size = min(nodes_by_queue_size.keys())
        
        # 如果有多个节点具有相同的最小队列长度，随机选择一个
        # 或者选择最近最少使用的节点（根据最后一次任务提交时间）
        candidates = nodes_by_queue_size[min_queue_size]
        if len(candidates) == 1:
            return candidates[0]
        else:
            # 选择最近最少使用的节点
            return min(candidates, key=lambda n: n.last_task_time)