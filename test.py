
from utils.config_utils import ConfigManager
from utils.redis_utils import RedisManager
config = ConfigManager('scheduler_config.json')
redis = RedisManager(config.get('redis', {}))
print(redis.delete("comfyui:task_id"))