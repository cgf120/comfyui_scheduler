import logging
import uuid
import docker
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("ComfyUI-Scheduler")

class DockerServer:
    """表示一个Docker服务器"""
    def __init__(self, server_id: str, host: str, port: int = 2375, 
                 max_containers: int = 8, gpu_ids: List[int] = [],
                 image: str = "",
                 volumes: Dict[str, Dict[str, str]] = {}):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.docker_url = f"tcp://{host}:{port}"
        self.max_containers = max_containers
        self.gpu_ids = gpu_ids or list(range(8))  # 默认8张显卡
        self.available_gpu_ids = self.gpu_ids.copy()
        self.containers = {}  # container_id -> gpu_id
        self.client = docker.DockerClient(base_url=self.docker_url)
        self.status = "initializing"
        # Add new configuration parameters
        self.image = image or "192.168.200.5/chenyu/public/c503abeeefb74af4ab4cb0e5948d4c56:v1.0.4-test-20250310"
        self.volumes = volumes or {
            "/mnt/pod-data/657442e3a84541f39df82091a53a6666": {"bind": "/poddata", "mode": "ro"},
            "/mnt/chenyu-nvme": {"bind": "/mnt/chenyu-nvme", "mode": "ro"},
            "/mnt/chenyu-nvme": {"bind": "/chenyudata", "mode": "ro"},
            "/mnt/user-data/store0/0/c9dbf7dd806c": {"bind": "/usrdata", "mode": "rw"},
            "/mnt/user-data/store0/0/c9dbf7dd806c/container/657442e3a84541f39df82091a53a6666": {"bind": "/app", "mode": "rw"}
        }

    async def start_comfyui_container(self, port: int) -> Tuple[Optional[str], Optional[int]]:
        """启动ComfyUI容器"""
        try:
            # 获取可用GPU
            gpu_id = self.get_available_gpu()
            if gpu_id is None:
                logger.warning(f"No available GPU on server {self.server_id}")
                return None, None
            
            # 创建容器
            container_name = f"comfyui-{uuid.uuid4().hex[:8]}"
            container_info = self.client.containers.run(
                image=self.image,
                name=container_name,
                ports={"8188": port},
                volumes=self.volumes,
                environment=[f"NVIDIA_VISIBLE_DEVICES={gpu_id}"],
                runtime="nvidia",
                detach=True  # 确保容器在后台运行
            )
            
            
            # 分配GPU
            # 确保container_info.id不为None后再调用allocate_gpu
            if container_info and container_info.id:
                self.allocate_gpu(gpu_id, container_info.id)
            else:
                logger.warning(f"Failed to start ComfyUI container on server {self.server_id}")
                return None, None
            
            logger.info(f"Started ComfyUI container {container_info.id} on server {self.server_id} with GPU {gpu_id}, port {port}")
            return container_info.id, gpu_id
        except Exception as e:
            logger.error(f"Failed to start ComfyUI container on server {self.server_id}: {str(e)}")
            return None, None
    
    async def stop_container(self, container_id: str):
        """停止并删除容器"""
        try:
            container = self.client.containers.get(container_id)
            container.stop(timeout=10)
            container.remove()
            self.release_gpu(container_id)
            logger.info(f"Stopped and removed container {container_id} on server {self.server_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to stop container {container_id} on server {self.server_id}: {str(e)}")
            return False
    def release_gpu(self, container_id: str):
        """释放容器占用的GPU"""
        if container_id in self.containers:
            gpu_id = self.containers[container_id]
            if gpu_id not in self.available_gpu_ids:
                self.available_gpu_ids.append(gpu_id)
            del self.containers[container_id]
    
    def close(self):
        """关闭Docker客户端连接"""
        if self.client:
            self.client.close()
            self.status = "closed"
    def allocate_gpu(self, gpu_id: int, container_id: str):
        """分配GPU给容器"""
        if gpu_id in self.available_gpu_ids:
            self.available_gpu_ids.remove(gpu_id)
            self.containers[container_id] = gpu_id
    async def initialize(self):
        """初始化Docker服务器连接"""
        try:
            # 创建Docker客户端
            self.client = docker.DockerClient(base_url=self.docker_url)
            # 测试连接
            self.client.ping()
            self.status = "running"
            logger.info(f"Docker server {self.server_id} ({self.host}) is connected")
            
            # 检查已有的容器
            containers = self.client.containers.list(all=True)
            for container in containers:
                if "comfyui" in container.name.lower():
                    # 获取容器使用的GPU
                    env = container.attrs.get('Config', {}).get('Env', [])
                    for e in env:
                        if e.startswith('NVIDIA_VISIBLE_DEVICES='):
                            gpu_id = int(e.split('=')[1])
                            if gpu_id in self.available_gpu_ids:
                                self.available_gpu_ids.remove(gpu_id)
                            self.containers[container.id] = gpu_id
            
            return True
        except Exception as e:
            self.status = "error"
            logger.error(f"Failed to connect to Docker server {self.server_id}: {str(e)}")
            return False
    def get_available_gpu(self) -> Optional[int]:
        """获取可用的GPU ID"""
        if not self.available_gpu_ids:
            return None
        return self.available_gpu_ids[0]