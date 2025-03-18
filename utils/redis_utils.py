import json
import logging
import redis
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger("ComfyUI-Scheduler")

class RedisManager:
    """Redis管理器，提供对Redis的操作封装"""
    
    def __init__(self,redis_config : Dict[str, Any]):
        """
        初始化Redis管理器
        
        Args:
            host: Redis服务器地址
            port: Redis服务器端口
            db: Redis数据库编号
            password: Redis密码
            decode_responses: 是否自动将响应解码为字符串
            workflow_key_prefix: 工作流键前缀
            task_key_prefix: 任务键前缀
        """
        self.host = redis_config.get("host", "localhost")
        self.port = redis_config.get("port", 6379)
        self.db = redis_config.get("db", 0)
        self.password = redis_config.get("password", None)
        self.decode_responses = redis_config.get("decode_responses", True)
        
        # 初始化Redis连接
        self._initialize_connection()
        
    def _initialize_connection(self):
        """初始化Redis连接"""
        try:
            self.redis = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=self.decode_responses
            )
            # 测试连接
            self.redis.ping()
            logger.info(f"Redis连接成功: {self.host}:{self.port}/{self.db}")
        except Exception as e:
            logger.error(f"Redis连接失败: {str(e)}")
            self.redis = None
    
    def is_connected(self) -> bool:
        """检查Redis连接是否可用"""
        if not self.redis:
            return False
        try:
            self.redis.ping()
            return True
        except:
            return False
    
    def reconnect(self) -> bool:
        """重新连接Redis"""
        try:
            self._initialize_connection()
            return self.is_connected()
        except Exception as e:
            logger.error(f"Redis重连失败: {str(e)}")
            return False
    
    # 键值操作
    def get(self, key: str) -> Any:
        """获取键值"""
        try:
            return self.redis.get(key)
        except Exception as e:
            logger.error(f"Redis get失败 {key}: {str(e)}")
            return None
    
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """设置键值，可选过期时间（秒）"""
        try:
            return self.redis.set(key, value, ex=ex)
        except Exception as e:
            logger.error(f"Redis set失败 {key}: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        """删除键"""
        try:
            return bool(self.redis.delete(key))
        except Exception as e:
            logger.error(f"Redis delete失败 {key}: {str(e)}")
            return False
    
    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        try:
            return bool(self.redis.exists(key))
        except Exception as e:
            logger.error(f"Redis exists失败 {key}: {str(e)}")
            return False
    
    def keys(self, pattern: str) -> List[str]:
        """获取匹配模式的所有键"""
        try:
            return self.redis.keys(pattern)
        except Exception as e:
            logger.error(f"Redis keys失败 {pattern}: {str(e)}")
            return []