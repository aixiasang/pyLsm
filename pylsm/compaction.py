"""
Compaction模块，实现LSM树的分层合并策略。

Compaction是LSM树的核心操作之一，用于将多个SSTable文件合并成更少、更大的文件，
以减少存储空间和提高查询效率。本模块实现了基于LevelDB的分层合并策略。
"""
import os
import shutil
from typing import List, Dict, Tuple, Optional, Set

from .sstable import SSTable
from .version_set import VersionSet
from .bloom_filter import BloomFilter
from .config import Config


class Compaction:
    """
    Compaction类，实现LSM树的分层合并策略。
    
    属性:
        db_dir: 数据库目录路径
        config: 配置对象
        version_set: 版本控制对象
    """
    
    def __init__(self, db_dir: str, version_set: VersionSet, config: Config):
        """
        初始化Compaction对象。
        
        Args:
            db_dir: 数据库目录路径
            version_set: 版本控制对象
            config: 配置对象
        """
        self.db_dir = db_dir
        self.version_set = version_set
        self.config = config
    
    def maybe_schedule_compaction(self) -> bool:
        """
        检查是否需要执行压缩，并在需要时安排一次压缩操作。
        
        Returns:
            如果安排了压缩操作，返回True；否则返回False
        """
        # 检查是否有层级需要压缩
        level = self.version_set.pick_compaction_level()
        if level < 0:
            return False  # 无需压缩
        
        # 执行指定层级的压缩
        return self.compact_level(level)
    
    def compact_level(self, level: int) -> bool:
        """
        执行指定层级的压缩操作。
        
        Args:
            level: 要压缩的层级
        
        Returns:
            操作是否成功
        """
        # 获取需要压缩的文件
        level_inputs, level_plus1_inputs = self.version_set.get_compaction_inputs(level)
        
        if not level_inputs:
            return False  # 没有需要压缩的文件
        
        # 合并文件，生成下一层的新文件
        output_files = self._merge_files(level_inputs, level_plus1_inputs, level + 1)
        
        if not output_files:
            return False  # 合并失败或没有输出
        
        # 应用压缩结果
        all_inputs = level_inputs + level_plus1_inputs
        self.version_set.apply_compaction_results(all_inputs, output_files)
        
        return True
    
    def _merge_files(self, level_files: List[str], level_plus1_files: List[str], 
                     target_level: int) -> List[Tuple[int, str]]:
        """
        合并两个层级的文件，生成新的SSTable文件。
        
        Args:
            level_files: 当前层级的文件路径列表
            level_plus1_files: 下一层级的文件路径列表
            target_level: 输出文件的目标层级
        
        Returns:
            生成的文件列表，每个元素为 (level, file_path) 元组
        """
        if not level_files:
            return []
        
        # 收集所有输入文件
        all_input_files = level_files + level_plus1_files
        
        # 从所有输入文件中读取数据
        merged_data = {}
        for file_path in all_input_files:
            abs_path = file_path if os.path.isabs(file_path) else os.path.join(self.db_dir, file_path)
            if os.path.exists(abs_path):
                try:
                    sstable = SSTable(abs_path)
                    for key, value in sstable.items():
                        # 如果键已存在，新值会覆盖旧值（因为level_files优先级更高）
                        merged_data[key] = value
                except Exception as e:
                    print(f"读取文件 {abs_path} 时出错: {e}")
        
        if not merged_data:
            return []  # 没有有效数据
        
        # 按照目标层级的大小限制拆分数据
        output_files = []
        current_data = {}
        current_size = 0
        target_file_size = self.config.compaction_level_target_file_size_base * (target_level + 1)
        
        # 按键排序
        sorted_keys = sorted(merged_data.keys())
        
        for key in sorted_keys:
            value = merged_data[key]
            key_size = len(key)
            value_size = len(value)
            entry_size = key_size + value_size + 8  # 8字节用于长度字段
            
            # 如果当前文件已满，创建新文件
            if current_size > 0 and current_size + entry_size > target_file_size:
                # 创建SSTable文件
                output_file = self._create_sst_file(current_data, target_level)
                if output_file:
                    output_files.append((target_level, output_file))
                
                # 重置当前数据
                current_data = {}
                current_size = 0
            
            # 添加当前键值对
            current_data[key] = value
            current_size += entry_size
        
        # 处理最后一个文件
        if current_data:
            output_file = self._create_sst_file(current_data, target_level)
            if output_file:
                output_files.append((target_level, output_file))
        
        return output_files
    
    def _create_sst_file(self, data: Dict[bytes, bytes], level: int) -> Optional[str]:
        """
        从数据创建一个新的SSTable文件。
        
        Args:
            data: 包含键值对的字典
            level: 文件所属层级
        
        Returns:
            创建的文件路径，如果失败则返回None
        """
        if not data:
            return None
        
        # 获取下一个SSTable路径
        file_path = self.version_set.get_next_sstable_path(level)
        abs_path = os.path.join(self.db_dir, file_path)
        
        # 创建布隆过滤器
        bloom_filter = BloomFilter(len(data), self.config.bloom_filter_false_positive_rate)
        for key in data.keys():
            bloom_filter.add(key)
        
        try:
            # 创建SSTable文件
            SSTable.write(abs_path, data, level, bloom_filter)
            return file_path
        except Exception as e:
            print(f"创建SSTable文件 {abs_path} 时出错: {e}")
            # 如果文件已创建但出错，尝试删除
            if os.path.exists(abs_path):
                try:
                    os.remove(abs_path)
                except OSError:
                    pass
            return None 