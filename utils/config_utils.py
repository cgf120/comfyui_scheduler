import os
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("ComfyUI-Scheduler")

class ConfigManager:
    """配置管理器，负责加载和保存配置"""
    
    def __init__(self, config_path: str = "scheduler_config.json"):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置字典
        """
        default_config = {}
        
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    # 合并默认配置
                    self._merge_default_config(config, default_config)
                    return config
            else:
                # 保存默认配置
                with open(self.config_path, 'w') as f:
                    json.dump(default_config, f, indent=2)
                return default_config
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return default_config
    
    def _merge_default_config(self, config: Dict[str, Any], default_config: Dict[str, Any]) -> None:
        """
        合并默认配置到当前配置
        
        Args:
            config: 当前配置
            default_config: 默认配置
        """
        for key, value in default_config.items():
            if key not in config:
                config[key] = value
            elif isinstance(value, dict) and isinstance(config[key], dict):
                # 递归合并嵌套字典
                self._merge_default_config(config[key], value)
    
    def save_config(self) -> bool:
        """
        保存配置到文件
        
        Returns:
            是否保存成功
        """
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info(f"Config saved to {self.config_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving config: {str(e)}")
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置项
        
        Args:
            key: 配置键
            default: 默认值
        
        Returns:
            配置值
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        设置配置项
        
        Args:
            key: 配置键
            value: 配置值
        """
        self.config[key] = value
    
    def get_nested(self, *keys: str, default: Any = None) -> Any:
        """
        获取嵌套配置项
        
        Args:
            keys: 配置键路径
            default: 默认值
        
        Returns:
            配置值
        """
        current = self.config
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                return default
            current = current[key]
        
        return current.get(keys[-1], default)
    
    def set_nested(self, *keys_and_value: Any) -> None:
        """
        设置嵌套配置项
        
        Args:
            keys_and_value: 配置键路径和值，最后一个参数为值
        """
        if len(keys_and_value) < 2:
            return
        
        keys = keys_and_value[:-1]
        value = keys_and_value[-1]
        
        current = self.config
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value