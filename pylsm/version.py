"""
版本控制和文件元数据管理模块。
"""

import os
import pickle
import threading
import time
from typing import Dict, List, Set, Optional, Tuple

from .config import Config, default_config


class FileMetaData:
    """
    表示SSTable文件的元数据。
    """
    
    def __init__(self, file_number: int, file_size: int, smallest: bytes, 
                largest: bytes, level: int = 0):
        """
        初始化文件元数据。
        
        Args:
            file_number: 文件编号。
            file_size: 文件大小（字节）。
            smallest: 文件中最小的键。
            largest: 文件中最大的键。
            level: 文件所在的层级（默认为0）。
        """
        self.file_number = file_number
        self.file_size = file_size
        self.smallest = smallest
        self.largest = largest
        self.level = level
    
    def overlaps(self, smallest: bytes, largest: bytes) -> bool:
        """
        检查文件的键范围是否与给定范围重叠。
        
        Args:
            smallest: 范围的最小键。
            largest: 范围的最大键。
            
        Returns:
            如果存在重叠返回True，否则返回False。
        """
        return not (self.largest < smallest or self.smallest > largest)


class VersionEdit:
    """
    表示版本之间的变更。
    """
    
    def __init__(self):
        """初始化版本编辑。"""
        self.added_files: Dict[int, List[FileMetaData]] = {}  # 按级别存储添加的文件
        self.deleted_files: Dict[int, Set[int]] = {}  # 按级别存储删除的文件编号
    
    def add_file(self, level: int, file_meta: FileMetaData) -> None:
        """
        添加文件到指定级别。
        
        Args:
            level: 文件所在的级别。
            file_meta: 文件元数据。
        """
        if level not in self.added_files:
            self.added_files[level] = []
        self.added_files[level].append(file_meta)
    
    def delete_file(self, level: int, file_number: int) -> None:
        """
        从指定级别删除文件。
        
        Args:
            level: 文件所在的级别。
            file_number: 要删除的文件编号。
        """
        if level not in self.deleted_files:
            self.deleted_files[level] = set()
        self.deleted_files[level].add(file_number)


class Version:
    """
    代表数据库在特定时间点的状态。
    """
    
    def __init__(self, config=None):
        """
        初始化版本。
        
        Args:
            config: 配置对象，如果为None则使用默认配置。
        """
        self.config = config if config is not None else default_config
        # 按层级存储文件元数据
        self.files: List[List[FileMetaData]] = [[] for _ in range(self.config.compaction_max_level)]
    
    def add_file(self, level: int, file_meta: FileMetaData) -> None:
        """
        添加文件到版本。
        
        Args:
            level: 层级。
            file_meta: 文件元数据。
        """
        # 确保级别有效
        if level >= len(self.files):
            for _ in range(level - len(self.files) + 1):
                self.files.append([])
        
        # 添加文件元数据
        self.files[level].append(file_meta)
        
        # 对Level 0以上的文件按键范围排序
        if level > 0:
            self.files[level].sort(key=lambda x: x.smallest)
    
    def delete_file(self, level: int, file_number: int) -> bool:
        """
        从版本中删除文件。
        
        Args:
            level: 层级。
            file_number: 文件编号。
            
        Returns:
            如果找到并删除文件返回True，否则返回False。
        """
        if level >= len(self.files):
            return False
        
        # 查找并删除文件
        for i, file_meta in enumerate(self.files[level]):
            if file_meta.file_number == file_number:
                self.files[level].pop(i)
                return True
        
        return False
    
    def get_overlapping_files(self, level: int, smallest: bytes, largest: bytes) -> List[FileMetaData]:
        """
        获取与给定范围重叠的文件。
        
        Args:
            level: 层级。
            smallest: 最小键。
            largest: 最大键。
            
        Returns:
            重叠的文件元数据列表。
        """
        result = []
        
        if level >= len(self.files):
            return result
        
        # 对于Level 0，所有文件可能有重叠
        if level == 0:
            for file_meta in self.files[level]:
                if file_meta.overlaps(smallest, largest):
                    result.append(file_meta)
            return result
        
        # 对于Level > 0，使用二分查找定位可能重叠的文件
        files = self.files[level]
        if not files:
            return result
        
        # 找到第一个可能重叠的文件
        index = 0
        while index < len(files) and files[index].largest < smallest:
            index += 1
        
        # 收集所有重叠的文件
        while index < len(files) and files[index].smallest <= largest:
            result.append(files[index])
            index += 1
        
        return result


class Compaction:
    """
    表示压缩操作。
    """
    
    def __init__(self, level: int, inputs: List[List[FileMetaData]], config=None):
        """
        初始化压缩操作。
        
        Args:
            level: 要压缩的层级。
            inputs: 输入文件列表，按层级分组。
            config: 配置对象，如果为None则使用默认配置。
        """
        self.level = level
        self.inputs = inputs
        self.config = config if config is not None else default_config


class VersionSet:
    """
    管理数据库的所有版本。
    """
    
    def __init__(self, db_path: str, config=None):
        """
        初始化版本集。
        
        Args:
            db_path: 数据库路径。
            config: 配置对象，如果为None则使用默认配置。
        """
        self.db_path = db_path
        self.config = config if config is not None else default_config
        self.current_version = Version(self.config)
        self.manifest_file = None
        self.manifest_file_number = 0
        self.next_file_number = 1
        self.mutex = threading.Lock()
        
        # 创建MANIFEST文件
        self._create_manifest()
    
    def _create_manifest(self) -> None:
        """创建MANIFEST文件。"""
        # 确保目录存在
        os.makedirs(self.db_path, exist_ok=True)
        
        manifest_path = os.path.join(self.db_path, f"MANIFEST-{self.manifest_file_number}")
        self.manifest_file = open(manifest_path, 'wb')
        
        # 将当前版本写入MANIFEST
        self._write_snapshot()
    
    def _write_snapshot(self) -> None:
        """将当前版本状态写入MANIFEST文件。"""
        if not self.manifest_file:
            return
        
        # 序列化当前版本
        snapshot = {
            'version': self.current_version.files,
            'next_file_number': self.next_file_number
        }
        
        # 写入MANIFEST
        pickle.dump(snapshot, self.manifest_file)
        self.manifest_file.flush()
        os.fsync(self.manifest_file.fileno())
    
    def apply(self, edit: VersionEdit) -> Version:
        """
        应用版本编辑，创建新版本。
        
        Args:
            edit: 版本编辑对象。
            
        Returns:
            新的当前版本。
        """
        with self.mutex:
            # 创建新版本
            new_version = Version(self.config)
            
            # 复制当前版本的文件
            for level, files in enumerate(self.current_version.files):
                new_version.files[level] = files.copy()
            
            # 应用删除
            for level, file_numbers in edit.deleted_files.items():
                for file_number in file_numbers:
                    new_version.delete_file(level, file_number)
            
            # 应用添加
            for level, files in edit.added_files.items():
                for file_meta in files:
                    new_version.add_file(level, file_meta)
            
            # 更新当前版本
            self.current_version = new_version
            
            # 更新MANIFEST
            self._write_snapshot()
            
            return self.current_version
    
    def current(self) -> Version:
        """
        获取当前版本。
        
        Returns:
            当前版本。
        """
        with self.mutex:
            return self.current_version
    
    def new_file_number(self) -> int:
        """
        获取新的文件编号。
        
        Returns:
            新文件编号。
        """
        with self.mutex:
            file_number = self.next_file_number
            self.next_file_number += 1
            return file_number
    
    def pick_compaction(self) -> Optional[Compaction]:
        """
        选择要进行压缩的文件。
        
        Returns:
            压缩对象或None（如果不需要压缩）。
        """
        version = self.current()
        
        # 策略1：如果Level 0文件数量过多，选择最老的文件
        if len(version.files[0]) >= self.config.compaction_level0_file_num_compaction_trigger:
            # 选择最老的文件（通常是文件编号最小的）
            oldest_files = sorted(version.files[0], key=lambda x: x.file_number)
            if oldest_files:
                # 获取重叠文件
                inputs = [oldest_files[:1]]  # Level 0的文件
                
                # 查找Level 1中与其重叠的文件
                smallest = oldest_files[0].smallest
                largest = oldest_files[0].largest
                inputs.append(version.get_overlapping_files(1, smallest, largest))
                
                return Compaction(0, inputs, self.config)
        
        # 策略2：选择大小超过阈值的层级
        for level in range(1, len(version.files)):
            level_size = sum(f.file_size for f in version.files[level])
            target_size = self.config.get_level_max_size(level)
            
            if level_size > target_size:
                # 选择该层级的第一个文件
                if version.files[level]:
                    file_to_compact = version.files[level][0]
                    inputs = [[file_to_compact]]  # 当前层级的文件
                    
                    # 查找下一层级中与其重叠的文件
                    smallest = file_to_compact.smallest
                    largest = file_to_compact.largest
                    inputs.append(version.get_overlapping_files(level + 1, smallest, largest))
                    
                    return Compaction(level, inputs, self.config)
        
        return None
    
    def close(self) -> None:
        """关闭版本集，释放资源。"""
        with self.mutex:
            if self.manifest_file:
                self.manifest_file.close()
                self.manifest_file = None 