import json
from typing import Dict, List, Any, Optional

class ComfyWorkflow:
    """ComfyUI工作流模型，用于存储和管理工作流信息"""
    
    def __init__(self, 
                 workflow_id: str,
                 name: str,
                 version: str,
                 prompt: Dict[str, Any],
                 input_nodes: Dict[str, Dict[str, Any]] = {},
                 output_nodes: List[str] = [],
                 description: str = "",
                 tags: List[str] = []):
        """
        初始化工作流
        
        Args:
            workflow_id: 工作流唯一ID
            name: 工作流名称
            version: 工作流版本
            prompt: 完整的工作流提交数据
            input_nodes: 输入节点配置，格式为 {node_id: {param_name: {type, display_name, default_value}}}
            output_nodes: 输出节点ID列表
            description: 工作流描述
            tags: 工作流标签
        """
        self.workflow_id = workflow_id
        self.name = name
        self.version = version
        self.prompt = prompt
        self.input_nodes = input_nodes or {}
        self.output_nodes = output_nodes or []
        self.description = description
        self.tags = tags or []
    
    def prepare_submission(self, input_values: Dict[str, Dict[str, Any]] = {}) -> Dict[str, Any]:
        """
        准备提交数据，替换输入参数
        
        Args:
            input_values: 输入参数值，格式为 {node_id: {param_name: value}}
        
        Returns:
            准备好的提交数据
        """
        if not input_values:
            return self.prompt.copy()
        
        # 深拷贝提交数据
        submission_data = json.loads(json.dumps(self.prompt))
        # 替换输入参数
        for node_id, params in input_values.items():
            if node_id in submission_data.get("prompt", {}):
                for param_name, value in params.items():
                    if param_name in submission_data["prompt"][node_id]["inputs"]:
                        submission_data["prompt"][node_id]["inputs"][param_name] = value
        
        return self.to_dict()
    
    def to_dict(self) -> Dict[str, Any]:
        """将工作流转换为字典"""
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "version": self.version,
            "prompt": self.prompt,
            "input_nodes": self.input_nodes,
            "output_nodes": self.output_nodes,
            "description": self.description,
            "tags": self.tags
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ComfyWorkflow':
        """从字典创建工作流"""
        return cls(
            workflow_id=data["workflow_id"],
            name=data["name"],
            version=data["version"],
            prompt=data["prompt"],
            input_nodes=data.get("input_nodes", {}),
            output_nodes=data.get("output_nodes", []),
            description=data.get("description", ""),
            tags=data.get("tags", [])
        )
    
    @classmethod
    def from_prompt(cls, 
                   workflow_id: str,
                   name: str,
                   version: str,
                   prompt: Dict[str, Any],
                   input_node_configs: List[Dict[str, Any]] = [],
                   output_node_ids: List[str] = [],
                   description: str = "",
                   tags: List[str] = []) -> 'ComfyWorkflow':
        """
        从提交数据创建工作流，并配置输入输出节点
        
        Args:
            workflow_id: 工作流唯一ID
            name: 工作流名称
            version: 工作流版本
            prompt: 完整的工作流提交数据
            input_node_configs: 输入节点配置列表，每项格式为 
                               {node_id, param_name, param_type, display_name, default_value}
            output_node_ids: 输出节点ID列表
            description: 工作流描述
            tags: 工作流标签
        
        Returns:
            工作流实例
        """
        input_nodes = {}
        
        if input_node_configs:
            for config in input_node_configs:
                node_id = config["node_id"]
                param_name = config["param_name"]
                
                if node_id not in input_nodes:
                    input_nodes[node_id] = {}
                
                input_nodes[node_id][param_name] = {
                    "type": config["param_type"],
                    "display_name": config["display_name"],
                    "default_value": config.get("default_value")
                }
        
        return cls(
            workflow_id=workflow_id,
            name=name,
            version=version,
            prompt=prompt,
            input_nodes=input_nodes,
            output_nodes=output_node_ids or [],
            description=description,
            tags=tags or []
        )