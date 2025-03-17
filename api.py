import logging
import uuid
import asyncio
import aiohttp
from aiohttp import web, WSMsgType
from typing import Dict

logger = logging.getLogger("ComfyUI-Scheduler")

def setup_routes(app, scheduler):
    """设置API路由"""
    routes = web.RouteTableDef()
    
    @routes.get("/nodes")
    async def get_nodes(request):
        """获取所有节点信息"""
        nodes_info = {}
        for node_id, node in scheduler.nodes.items():
            nodes_info[node_id] = {
                "host": node.host,
                "port": node.port,
                "url": node.url,
                "status": node.status,
                "queue_size": node.queue_size,
                "last_heartbeat": node.last_heartbeat,
                "is_available": node.is_available(),
                "is_local": node.is_local
            }
        return web.json_response(nodes_info)
    
    @routes.post("/prompt")
    async def submit_prompt(request):
        """提交提示到调度器"""
        try:
            data = await request.json()
            task_id = str(uuid.uuid4())
            await scheduler.task_queue.put((task_id, data))
            return web.json_response({"task_id": task_id, "status": "queued"})
        except Exception as e:
            logger.error(f"Error submitting prompt: {str(e)}")
            return web.json_response({"error": str(e)}, status=400)
    
    @routes.post("/upload")
    async def upload_file(request):
        """上传文件到ComfyUI节点"""
        try:
            # 获取表单数据
            reader = await request.multipart()
            field = await reader.next()
            
            if field is None:
                return web.json_response({"error": "No file provided"}, status=400)
            
            # 获取文件名和内容
            filename = field.filename
            if not filename:
                return web.json_response({"error": "No filename provided"}, status=400)
            
            # 获取目标节点ID
            node_id = request.query.get('node_id')
            if not node_id or node_id not in scheduler.nodes:
                return web.json_response({"error": "Valid node_id is required"}, status=400)
            
            node = scheduler.nodes[node_id]
            
            # 读取文件内容
            file_content = bytearray()
            while True:
                chunk = await field.read_chunk()
                if not chunk:
                    break
                file_content.extend(chunk)
            
            # 构建上传请求
            upload_url = f"http://{node.host}:{node.port}/upload/image"
            
            # 发送文件到ComfyUI节点
            form = aiohttp.FormData()
            form.add_field('image', 
                          file_content,
                          filename=filename,
                          content_type='application/octet-stream')
            
            async with aiohttp.ClientSession() as session:
                async with session.post(upload_url, data=form) as response:
                    if response.status == 200:
                        result = await response.json()
                        return web.json_response({
                            "success": True,
                            "filename": filename,
                            "node_id": node_id,
                            "result": result
                        })
                    else:
                        error_text = await response.text()
                        return web.json_response({
                            "success": False,
                            "error": f"Upload failed: {error_text}"
                        }, status=response.status)
                        
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            return web.json_response({"error": str(e)}, status=400)
    
    @routes.post("/nodes")
    async def add_node(request):
        """手动添加节点"""
        try:
            data = await request.json()
            host = data.get("host")
            port = data.get("port")
            
            if not host or not port:
                return web.json_response({"error": "Host and port are required"}, status=400)
            
            node_id = str(uuid.uuid4())
            node = scheduler.create_node(
                node_id=node_id,
                host=host,
                port=port,
                max_queue_size=data.get("max_queue_size", 5)
            )
            
            if await node.initialize():
                scheduler.nodes[node_id] = node
                asyncio.create_task(scheduler._monitor_node(node))
                return web.json_response({"node_id": node_id, "status": "added"})
            else:
                return web.json_response({"error": "Failed to initialize node"}, status=400)
        except Exception as e:
            logger.error(f"Error adding node: {str(e)}")
            return web.json_response({"error": str(e)}, status=400)
    
    @routes.delete("/nodes/{node_id}")
    async def remove_node(request):
        """手动移除节点"""
        node_id = request.match_info.get("node_id")
        if node_id in scheduler.nodes:
            node = scheduler.nodes[node_id]
            await node.close()
            del scheduler.nodes[node_id]
            return web.json_response({"status": "removed"})
        else:
            return web.json_response({"error": "Node not found"}, status=404)
    
    @routes.get("/config")
    async def get_config(request):
        """获取当前配置"""
        return web.json_response(scheduler.config)
    
    @routes.post("/config")
    async def update_config(request):
        """更新配置"""
        try:
            data = await request.json()
            # 更新配置
            for key, value in data.items():
                if key in scheduler.config:
                    scheduler.config[key] = value
            
            # 保存配置
            scheduler.save_config()
            
            return web.json_response({"status": "updated"})
        except Exception as e:
            logger.error(f"Error updating config: {str(e)}")
            return web.json_response({"error": str(e)}, status=400)
    
    @routes.get("/docker-servers")
    async def get_docker_servers(request):
        """获取所有Docker服务器信息"""
        servers_info = {}
        for server_id, server in scheduler.docker_servers.items():
            servers_info[server_id] = {
                "host": server.host,
                "port": server.port,
                "status": server.status,
                "max_containers": server.max_containers,
                "available_gpus": len(server.available_gpu_ids),
                "total_gpus": len(server.gpu_ids),
                "containers": len(server.containers)
            }
        return web.json_response(servers_info)
    
    @routes.post("/docker-servers")
    async def add_docker_server(request):
        """手动添加Docker服务器"""
        try:
            data = await request.json()
            host = data.get("host")
            
            if not host:
                return web.json_response({"error": "Host is required"}, status=400)
            
            server_id = str(uuid.uuid4())
            server = scheduler.create_docker_server(
                server_id=server_id,
                host=host,
                port=data.get("port", 2375),
                max_containers=data.get("max_containers", 8),
                gpu_ids=data.get("gpu_ids", list(range(8)))
            )
            
            if await server.initialize():
                scheduler.docker_servers[server_id] = server
                return web.json_response({"server_id": server_id, "status": "added"})
            else:
                return web.json_response({"error": "Failed to initialize Docker server"}, status=400)
        except Exception as e:
            logger.error(f"Error adding Docker server: {str(e)}")
            return web.json_response({"error": str(e)}, status=400)
    
    @routes.delete("/docker-servers/{server_id}")
    async def remove_docker_server(request):
        """手动移除Docker服务器"""
        server_id = request.match_info.get("server_id")
        if server_id in scheduler.docker_servers:
            server = scheduler.docker_servers[server_id]
            server.close()
            del scheduler.docker_servers[server_id]
            return web.json_response({"status": "removed"})
        else:
            return web.json_response({"error": "Docker server not found"}, status=404)
    
    @routes.post("/docker-nodes")
    async def start_docker_node(request):
        """在Docker服务器上启动ComfyUI节点"""
        try:
            data = await request.json()
            server_id = data.get("server_id")
            
            if not server_id or server_id not in scheduler.docker_servers:
                return web.json_response({"error": "Valid server_id is required"}, status=400)
            
            server = scheduler.docker_servers[server_id]
            port = data.get("port", scheduler.node_port_start + len(scheduler.nodes))
            
            container_id, gpu_id = await server.start_comfyui_container(port)
            if not container_id:
                return web.json_response({"error": "Failed to start container"}, status=400)
            
            # 创建节点
            node_id = str(uuid.uuid4())
            node = scheduler.create_node(
                node_id=node_id,
                host=server.host,
                port=port,
                max_queue_size=data.get("max_queue_size", 5),
                container_id=container_id,
                server_id=server_id
            )
            
            # 等待节点初始化
            max_retries = 30
            for i in range(max_retries):
                if await node.initialize():
                    scheduler.nodes[node_id] = node
                    asyncio.create_task(scheduler._monitor_node(node))
                    return web.json_response({
                        "node_id": node_id, 
                        "container_id": container_id,
                        "gpu_id": gpu_id,
                        "status": "started"
                    })
                await asyncio.sleep(1)
            
            # 如果无法初始化，停止容器
            await server.stop_container(container_id)
            return web.json_response({"error": "Failed to initialize node"}, status=400)
        except Exception as e:
            logger.error(f"Error starting Docker node: {str(e)}")
            return web.json_response({"error": str(e)}, status=400)
    
    @routes.delete("/docker-nodes/{node_id}")
    async def stop_docker_node(request):
        """停止Docker节点"""
        node_id = request.match_info.get("node_id")
        if node_id in scheduler.nodes:
            node = scheduler.nodes[node_id]
            
            # 检查是否是Docker容器节点
            if node.container_id and node.server_id:
                # 停止容器
                if node.server_id in scheduler.docker_servers:
                    server = scheduler.docker_servers[node.server_id]
                    await server.stop_container(node.container_id)
                
                # 关闭节点
                await node.close()
                del scheduler.nodes[node_id]
                return web.json_response({"status": "stopped"})
            else:
                return web.json_response({"error": "Not a Docker node"}, status=400)
        else:
            return web.json_response({"error": "Node not found"}, status=404)
    
    app.add_routes(routes)
    return app