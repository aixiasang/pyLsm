"""
VersionSet模块，用于管理SSTable文件的版本和层级结构。

LSM树的层级结构如下：
- Level-0：由MemTable直接刷写到磁盘，文件之间可能有键范围重叠
- Level-1及以上：每一层文件的键范围不重叠，层级越高文件越大

每一层的大小限制遵循以下规则：
- Level-0：文件数量达到阈值时触发压缩
- Level-1及以上：每一层总大小约为上一层的10倍
"""
import os
import json
import re
import glob
import time
import struct
import shutil
import threading
from typing import List, Dict, Tuple, Set, Optional, Any

from .sstable import SSTable
from .bloom_filter import BloomFilter

# 版本相关常量
LEVEL_NUMBER = 7                   # 层级数
LEVEL0_MAX_FILES = 8               # Level 0最大文件数
LEVEL0_COMPACTION_TRIGGER = 4      # Level 0触发压缩的文件数
LEVEL_SIZE_MULTIPLIER = 10         # 每层大小倍数（除Level 0外）
LEVEL0_SIZE = 4 * 1024 * 1024      # Level 0的基准大小（4MB）

class FileMetaData:
    """
    SSTable文件元数据，包含文件信息。
    """
    def __init__(self, file_number: int, file_size: int, 
                 smallest_key: bytes, largest_key: bytes, level: int = 0):
        """
        初始化SSTable文件元数据。
        
        参数：
            file_number: 文件编号
            file_size: 文件大小（字节）
            smallest_key: 最小键
            largest_key: 最大键
            level: 文件所在层级，默认为0
        """
        self.file_number = file_number
        self.file_size = file_size
        self.smallest_key = smallest_key
        self.largest_key = largest_key
        self.level = level
    
    def overlaps_with(self, smallest_key: bytes, largest_key: bytes) -> bool:
        """
        检查此文件是否与给定的键范围重叠。
        
        参数：
            smallest_key: 范围的最小键
            largest_key: 范围的最大键
            
        返回：
            如果有重叠则为True，否则为False
        """
        return self.smallest_key <= largest_key and self.largest_key >= smallest_key
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将文件元数据转换为字典表示。
        
        返回：
            包含文件元数据的字典
        """
        return {
            'file_number': self.file_number,
            'file_size': self.file_size,
            'smallest_key': self.smallest_key.hex(),
            'largest_key': self.largest_key.hex(),
            'level': self.level
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileMetaData':
        """
        从字典创建文件元数据对象。
        
        参数：
            data: 包含文件元数据的字典
            
        返回：
            新的FileMetaData实例
        """
        smallest_key = data['smallest_key']
        largest_key = data['largest_key']
        
        # 检查键是否已经是字节类型
        if isinstance(smallest_key, str):
            smallest_key = bytes.fromhex(smallest_key)
        
        if isinstance(largest_key, str):
            largest_key = bytes.fromhex(largest_key)
        
        return cls(
            file_number=data['file_number'],
            file_size=data['file_size'],
            smallest_key=smallest_key,
            largest_key=largest_key,
            level=data.get('level', 0)
        )


class Version:
    """
    表示数据库的一个版本，包含该版本中的所有文件。
    """
    
    def __init__(self, version_set: 'VersionSet', version_number: int):
        """
        初始化版本。
        
        参数：
            version_set: 版本集合
            version_number: 版本号
        """
        self.version_set = version_set
        self.version_number = version_number
        self.files = [[] for _ in range(LEVEL_NUMBER)]  # 每一层的文件列表
        
    def add_file(self, level: int, file_meta: FileMetaData) -> None:
        """
        添加一个文件到指定层级。
        
        参数：
            level: 层级
            file_meta: 文件元数据
        """
        # 检查level是否超出范围
        if level >= LEVEL_NUMBER:
            print(f"警告: 尝试添加文件到超出范围的level {level}，调整为最大level {LEVEL_NUMBER-1}")
            level = LEVEL_NUMBER - 1
        
        # 添加文件
        self.files[level].append(file_meta)
        
        # 对于非0层的文件，按照最小键排序
        if level > 0:
            self.files[level].sort(key=lambda x: x.smallest_key)
    
    def get_overlapping_files(self, level: int, smallest_key: bytes, largest_key: bytes) -> List[FileMetaData]:
        """
        获取指定层级中与给定键范围重叠的所有文件。
        
        参数：
            level: 层级
            smallest_key: 范围的最小键
            largest_key: 范围的最大键
            
        返回：
            重叠的文件列表
        """
        result = []
        
        if level < 0 or level >= self.MAX_LEVELS:
            return result
        
        # Level 0的文件可能彼此重叠，需要检查所有文件
        if level == 0:
            for file_meta in self.files[level]:
                if file_meta.overlaps_with(smallest_key, largest_key):
                    result.append(file_meta)
        else:
            # 非0层的文件是有序的、非重叠的，可以使用二分查找
            # 在实际实现中，这里应该用二分查找来优化，简化起见，暂时用线性查找
            for file_meta in self.files[level]:
                if file_meta.overlaps_with(smallest_key, largest_key):
                    result.append(file_meta)
        
        return result
    
    def get_level_size(self, level: int) -> int:
        """
        获取指定层级的总文件大小。
        
        参数：
            level: 层级
            
        返回：
            层级的总大小（字节）
        """
        return sum(f.file_size for f in self.files[level])
    
    def get_file_path(self, file_number: int) -> str:
        """
        获取SSTable文件的完整路径。
        
        参数：
            file_number: 文件编号
            
        返回：
            文件的完整路径
        """
        return os.path.join(self.version_set.db_path, f"{file_number}.sst")
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将版本转换为可序列化的字典。
        
        返回：
            表示版本的字典
        """
        result = {
            'version_number': self.version_number,
            'files': []
        }
        
        # 将所有文件元数据添加到列表
        for level, level_files in enumerate(self.files):
            for file_meta in level_files:
                file_dict = file_meta.to_dict()
                result['files'].append(file_dict)
        
        return result
    
    @classmethod
    def from_dict(cls, version_set: 'VersionSet', data: Dict[str, Any]) -> 'Version':
        """
        从字典创建版本对象。
        
        参数：
            version_set: 版本集合
            data: 包含版本数据的字典
            
        返回：
            新的Version实例
        """
        version = cls(version_set, data['version_number'])
        
        # 添加所有文件
        for file_dict in data['files']:
            file_meta = FileMetaData.from_dict(file_dict)
            version.add_file(file_meta.level, file_meta)
        
        return version
    
    def needs_compaction(self) -> bool:
        """
        检查此版本是否需要进行压缩。
        
        返回：
            如果需要压缩则为True，否则为False
        """
        # 检查Level 0文件数量
        if len(self.files[0]) >= self.LEVEL0_TARGET_FILE_COUNT:
            return True
        
        # 检查其他层级的大小
        for level in range(1, self.MAX_LEVELS - 1):
            target_size = self.LEVEL0_TARGET_FILE_COUNT * (self.LEVEL_SIZE_MULTIPLIER ** level) * 1024 * 1024  # MB
            if self.get_level_size(level) > target_size:
                return True
        
        return False
    
    def pick_compaction_files(self) -> Tuple[int, List[FileMetaData], List[FileMetaData]]:
        """
        选择需要进行压缩的文件。
        
        返回：
            元组 (level, level_n_files, level_n_plus_1_files)，其中：
            - level: 要压缩的起始层级
            - level_n_files: 层级n中要压缩的文件列表
            - level_n_plus_1_files: 层级n+1中与level_n_files重叠的文件列表
        """
        # 先检查Level 0是否需要压缩
        if len(self.files[0]) >= self.LEVEL0_TARGET_FILE_COUNT:
            level = 0
            # 对于Level 0，选择所有文件
            level_n_files = self.files[0].copy()
            
            # 找出与Level 0文件重叠的Level 1文件
            # 计算Level 0所有文件覆盖的键范围
            smallest_key = min(f.smallest_key for f in level_n_files)
            largest_key = max(f.largest_key for f in level_n_files)
            
            # 获取Level 1中与这个范围重叠的文件
            level_n_plus_1_files = self.get_overlapping_files(1, smallest_key, largest_key)
            
            return level, level_n_files, level_n_plus_1_files
        
        # 检查其他层级
        for level in range(1, self.MAX_LEVELS - 1):
            target_size = self.LEVEL0_TARGET_FILE_COUNT * (self.LEVEL_SIZE_MULTIPLIER ** level) * 1024 * 1024  # MB
            if self.get_level_size(level) > target_size:
                # 选择最旧的文件（简化实现，实际可以有更复杂的策略）
                level_n_files = [self.files[level][0]] if self.files[level] else []
                
                if not level_n_files:
                    continue
                
                # 找出与选定文件重叠的下一层文件
                level_n_plus_1_files = self.get_overlapping_files(
                    level + 1, 
                    level_n_files[0].smallest_key, 
                    level_n_files[0].largest_key
                )
                
                return level, level_n_files, level_n_plus_1_files
        
        # 如果没有需要压缩的层级，返回空
        return -1, [], []


class VersionEdit:
    """
    表示版本之间的变更。
    """
    
    def __init__(self):
        """初始化版本编辑。"""
        self.deleted_files = []  # 存储(level, file_number)元组，表示要删除的文件
        self.new_files = []      # 存储(level, file_meta)元组，表示要添加的文件
        self.next_file_number = None  # 下一个可用的文件编号
        self.last_sequence = None     # 最后使用的序列号
    
    def add_file(self, level: int, file_meta: FileMetaData) -> None:
        """
        添加一个文件到指定级别。
        
        参数：
            level: 层级
            file_meta: 文件元数据
        """
        self.new_files.append((level, file_meta))
    
    def delete_file(self, level: int, file_number: int) -> None:
        """
        从指定级别删除一个文件。
        
        参数：
            level: 层级
            file_number: 文件编号
        """
        self.deleted_files.append((level, file_number))
    
    def set_next_file_number(self, file_number: int) -> None:
        """
        设置下一个文件编号。
        
        参数：
            file_number: 下一个文件编号
        """
        self.next_file_number = file_number
    
    def set_last_sequence(self, sequence: int) -> None:
        """
        设置最后的序列号。
        
        参数：
            sequence: 序列号
        """
        self.last_sequence = sequence
    
    def to_dict(self) -> Dict:
        """
        将版本编辑转换为字典。
        
        返回：
            表示版本编辑的字典
        """
        result = {
            'deleted_files': [],
            'new_files': []
        }
        
        # 添加删除的文件
        for level, file_number in self.deleted_files:
            result['deleted_files'].append([level, file_number])
        
        # 添加新文件
        for level, file_meta in self.new_files:
            file_dict = {
                'level': level,
                'file_meta': {
                    'file_number': file_meta.file_number,
                    'file_size': file_meta.file_size,
                    'smallest_key': file_meta.smallest_key.hex(),
                    'largest_key': file_meta.largest_key.hex(),
                    'level': file_meta.level
                }
            }
            result['new_files'].append(file_dict)
        
        # 添加其他字段
        if self.next_file_number is not None:
            result['next_file_number'] = self.next_file_number
        
        if self.last_sequence is not None:
            result['last_sequence'] = self.last_sequence
        
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VersionEdit':
        """
        从字典创建版本编辑对象。
        
        参数：
            data: 包含版本编辑数据的字典
            
        返回：
            新的VersionEdit实例
        """
        edit = cls()
        
        if 'deleted_files' in data:
            for file_data in data['deleted_files']:
                if isinstance(file_data, list):
                    level, file_number = file_data
                    edit.delete_file(level, file_number)
                else:
                    level, file_number = file_data
                    edit.delete_file(level, file_number)
        
        if 'new_files' in data:
            for file_data in data['new_files']:
                if isinstance(file_data, dict) and 'level' in file_data and 'file_meta' in file_data:
                    # 新格式
                    level = file_data['level']
                    file_meta_dict = file_data['file_meta']
                    
                    # 创建FileMetaData对象
                    file_meta = FileMetaData(
                        file_number=file_meta_dict['file_number'],
                        file_size=file_meta_dict['file_size'],
                        smallest_key=bytes.fromhex(file_meta_dict['smallest_key']),
                        largest_key=bytes.fromhex(file_meta_dict['largest_key']),
                        level=file_meta_dict.get('level', level)
                    )
                    
                    edit.add_file(level, file_meta)
                elif isinstance(file_data, tuple) and len(file_data) == 2:
                    # 旧格式 (level, file_meta_dict)
                    level, file_meta_dict = file_data
                    file_meta = FileMetaData.from_dict(file_meta_dict)
                    edit.add_file(level, file_meta)
        
        if 'next_file_number' in data:
            edit.set_next_file_number(data['next_file_number'])
        
        if 'last_sequence' in data:
            edit.set_last_sequence(data['last_sequence'])
        
        return edit


class VersionSet:
    """版本集合，管理数据库的所有版本。"""
    
    def __init__(self, db_path: str):
        """
        初始化版本集合。
        
        参数：
            db_path: 数据库目录路径
        """
        self.db_path = db_path
        self.mutex = threading.RLock()  # 用于线程安全操作的互斥锁
        self.next_file_number = 1  # 下一个可用的文件编号
        self.last_sequence = 0      # 最后使用的序列号
        self.manifest_file = None   # MANIFEST文件
        self.manifest_file_number = 0  # MANIFEST文件编号
        self.current = None         # 当前版本
        self.current_version_number = 0  # 当前版本号
        self.versions = []          # 所有版本列表
        
    def append_version(self, version: Version) -> None:
        """
        添加一个新版本到版本列表。
        
        参数：
            version: 新的版本
        """
        self.versions.append(version)
        self.current = version
        self.current_version_number = version.version_number
        
    def recover(self) -> bool:
        """
        从磁盘恢复版本集合。
        
        返回：
            是否成功恢复
        """
        try:
            # 创建初始版本
            initial_version = Version(self, 0)
            self.append_version(initial_version)
            
            # 查找MANIFEST文件
            manifest_path = os.path.join(self.db_path, "MANIFEST")
            if not os.path.exists(manifest_path):
                print("未找到MANIFEST文件，使用初始版本")
                self.create_manifest()
                return True
            
            # 打开并读取MANIFEST文件
            with open(manifest_path, 'r') as f:
                lines = f.readlines()
            
            if not lines:
                print("MANIFEST文件为空，使用初始版本")
                return True
            
            # 应用所有版本编辑
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    edit = VersionEdit.from_dict(data)
                    self.apply_version_edit(edit)
                except json.JSONDecodeError as e:
                    print(f"读取MANIFEST文件失败: {e}")
                    continue
                except Exception as e:
                    print(f"应用版本编辑失败: {e}")
                    continue
            
            # 重用已有的MANIFEST文件
            self.manifest_file = open(manifest_path, 'a')
            
            return True
        except Exception as e:
            print(f"恢复版本集合失败: {e}")
            import traceback
            traceback.print_exc()
            return False
            
    def create_manifest(self) -> None:
        """创建新的MANIFEST文件。"""
        try:
            manifest_path = os.path.join(self.db_path, "MANIFEST")
            self.manifest_file = open(manifest_path, 'w')
            
            # 写入初始版本
            initial_edit = VersionEdit()
            initial_edit.set_next_file_number(self.next_file_number)
            initial_edit.set_last_sequence(self.last_sequence)
            
            json_str = json.dumps(initial_edit.to_dict())
            self.manifest_file.write(json_str + '\n')
            self.manifest_file.flush()
        except Exception as e:
            print(f"创建MANIFEST文件失败: {e}")
            
    def close(self) -> None:
        """关闭版本集合，释放资源。"""
        try:
            if self.manifest_file:
                self.manifest_file.close()
                self.manifest_file = None
        except Exception as e:
            print(f"关闭MANIFEST文件失败: {e}")
    
    def __del__(self) -> None:
        """析构函数，确保资源释放。"""
        self.close()
    
    def new_file_number(self) -> int:
        """
        生成并返回一个新的文件编号。
        
        返回：
            新的文件编号
        """
        with self.mutex:
            file_number = self.next_file_number
            self.next_file_number += 1
            return file_number
    
    def get_next_file_number(self) -> int:
        """
        获取下一个可用的文件编号。
        
        返回：
            下一个可用的文件编号
        """
        return self.new_file_number()
        
    def get_current(self) -> Version:
        """
        获取当前版本。
        
        返回：
            当前版本
        """
        return self.current
        
    def get_last_sequence(self) -> int:
        """
        获取最后使用的序列号。
        
        返回：
            最后使用的序列号
        """
        return self.last_sequence
        
    def set_last_sequence(self, sequence: int) -> None:
        """
        设置最后使用的序列号。
        
        参数：
            sequence: 序列号
        """
        self.last_sequence = sequence

    def get_current_version(self) -> Version:
        """
        获取当前版本。
        
        返回：
            当前版本
        """
        return self.current
    
    def needs_compaction(self) -> bool:
        """
        检查是否需要进行压缩。
        
        返回：
            如果需要压缩则为True，否则为False
        """
        if not self.current:
            return False
        
        # 检查Level 0文件数量
        if len(self.current.files[0]) > LEVEL0_MAX_FILES:
            return True
            
        # 检查其他层级的大小
        for level in range(1, LEVEL_NUMBER - 1):
            if self._level_size(level) > self._max_level_size(level):
                return True
        
        return False
    
    def _level_size(self, level: int) -> int:
        """
        计算指定层级的总大小。
        
        参数：
            level: 层级
            
        返回：
            层级的总文件大小（字节）
        """
        total_size = 0
        for file_meta in self.current.files[level]:
            total_size += file_meta.file_size
        return total_size
    
    def _max_level_size(self, level: int) -> int:
        """
        计算指定层级的最大允许大小。
        
        参数：
            level: 层级
            
        返回：
            层级的最大允许大小（字节）
        """
        return LEVEL_SIZE_MULTIPLIER**(level-1) * LEVEL0_SIZE
    
    def pick_compaction_files(self) -> Tuple[int, List[FileMetaData], List[FileMetaData]]:
        """
        选择需要进行压缩的文件。
        
        返回：
            元组 (level, level_n_files, level_n_plus_1_files)
        """
        # 检查Level 0是否需要压缩
        if len(self.current.files[0]) > LEVEL0_MAX_FILES:
            return self._pick_level0_compaction()
        
        # 检查其他层级
        for level in range(1, LEVEL_NUMBER - 1):
            if self._level_size(level) > self._max_level_size(level):
                return self._pick_level_compaction(level)
        
        # 默认压缩层级0
        return self._pick_level0_compaction()
    
    def _pick_level0_compaction(self) -> Tuple[int, List[FileMetaData], List[FileMetaData]]:
        """
        选择Level 0进行压缩。
        
        返回：
            元组 (level, level_0_files, level_1_files)
        """
        level = 0
        level_files = self.current.files[level][:LEVEL0_COMPACTION_TRIGGER]
        
        if not level_files:
            return level, [], []
        
        # 确定键范围
        smallest_key = min(f.smallest_key for f in level_files)
        largest_key = max(f.largest_key for f in level_files)
        
        # 获取下一层重叠的文件
        next_level_files = self.current.get_overlapping_files(level + 1, smallest_key, largest_key)
        
        return level, level_files, next_level_files
    
    def _pick_level_compaction(self, level: int) -> Tuple[int, List[FileMetaData], List[FileMetaData]]:
        """
        选择指定层级进行压缩。
        
        参数：
            level: 要压缩的层级
            
        返回：
            元组 (level, level_n_files, level_n_plus_1_files)
        """
        if not self.current.files[level]:
            return level, [], []
        
        # 简单地选择一个文件进行压缩
        file_to_compact = self.current.files[level][0]
        level_files = [file_to_compact]
        
        # 获取下一层重叠的文件
        next_level_files = self.current.get_overlapping_files(
            level + 1, file_to_compact.smallest_key, file_to_compact.largest_key
        )
        
        return level, level_files, next_level_files
        
    def apply_version_edit(self, edit: VersionEdit) -> bool:
        """
        应用版本编辑，创建新版本。
        
        参数：
            edit: 版本编辑
            
        返回：
            是否成功应用版本编辑
        """
        try:
            # 创建新版本
            new_version = Version(self, self.current_version_number + 1)
            
            # 复制当前版本文件到新版本
            for level in range(LEVEL_NUMBER):
                for file_meta in self.current.files[level]:
                    # 检查该文件是否被删除
                    deleted = False
                    for deleted_level, deleted_file_number in edit.deleted_files:
                        if deleted_level == level and deleted_file_number == file_meta.file_number:
                            deleted = True
                            break
                    
                    if not deleted:
                        new_version.add_file(level, file_meta)
            
            # 添加新文件
            for level, file_meta in edit.new_files:
                new_version.add_file(level, file_meta)
            
            # 更新状态
            if edit.next_file_number is not None:
                self.next_file_number = max(self.next_file_number, edit.next_file_number)
            
            if edit.last_sequence is not None:
                self.last_sequence = max(self.last_sequence, edit.last_sequence)
            
            # 写入日志记录版本变更
            try:
                if self.manifest_file:
                    json_str = json.dumps(edit.to_dict())
                    self.manifest_file.write(json_str + '\n')
                    self.manifest_file.flush()
            except Exception as e:
                print(f"写入MANIFEST文件失败: {e}")
                return False
            
            # 更新当前版本
            self.append_version(new_version)
            return True
        except Exception as e:
            print(f"应用版本编辑失败: {e}")
            import traceback
            traceback.print_exc()
            return False


class Compaction:
    """
    压缩操作，用于合并SSTable文件。
    """
    def __init__(self, 
                 version_set: VersionSet, 
                 level: int, 
                 input_files_level_n: List[FileMetaData], 
                 input_files_level_n_plus_1: List[FileMetaData]):
        """
        初始化压缩操作。
        
        参数：
            version_set: 版本集
            level: 要压缩的起始层级
            input_files_level_n: 层级n中要压缩的文件
            input_files_level_n_plus_1: 层级n+1中与level_n_files重叠的文件
        """
        self.version_set = version_set
        self.level = level
        self.input_files_level_n = input_files_level_n
        self.input_files_level_n_plus_1 = input_files_level_n_plus_1
        self.edit = VersionEdit()
    
    def _merge_files(self) -> Tuple[Dict[bytes, bytes], bytes, bytes]:
        """
        合并所有输入文件的内容。
        
        返回：
            元组 (merged_data, smallest_key, largest_key)
        """
        merged_data = {}
        all_keys = set()
        
        # 打开并读取所有文件
        # 先处理level_n的文件
        for file_meta in self.input_files_level_n:
            file_path = os.path.join(self.version_set.db_path, f"{file_meta.file_number}.sst")
            sst = SSTable(file_path)
            
            # 读取所有键值对
            for key, value in sst.get_iterator():
                merged_data[key] = value
                all_keys.add(key)
            
            # 关闭文件
            sst.close()
            
            # 将此文件标记为已删除
            self.edit.delete_file(self.level, file_meta.file_number)
        
        # 然后处理level_n+1的文件
        for file_meta in self.input_files_level_n_plus_1:
            file_path = os.path.join(self.version_set.db_path, f"{file_meta.file_number}.sst")
            sst = SSTable(file_path)
            
            # 读取所有键值对
            for key, value in sst.get_iterator():
                # 只有在合并范围内的键才处理
                if key in all_keys:
                    merged_data[key] = value
                
                all_keys.add(key)
            
            # 关闭文件
            sst.close()
            
            # 将此文件标记为已删除
            self.edit.delete_file(self.level + 1, file_meta.file_number)
        
        # 确定最小和最大键
        if not all_keys:
            return {}, b'', b''
        
        smallest_key = min(all_keys)
        largest_key = max(all_keys)
        
        return merged_data, smallest_key, largest_key
    
    def compact(self) -> bool:
        """
        执行压缩操作。
        
        返回：
            如果成功则为True，否则为False
        """
        try:
            print(f"开始压缩层级 {self.level} 到 {self.level+1}")
            
            # 合并文件内容
            merged_data, smallest_key, largest_key = self._merge_files()
            
            if not merged_data:
                print("没有数据需要合并")
                return True
            
            # 创建新的SSTable文件
            new_file_number = self.version_set.get_next_file_number()
            new_file_path = os.path.join(self.version_set.db_path, f"{new_file_number}.sst")
            
            # 确定输出文件的层级（总是level+1，除了level 0）
            output_level = self.level + 1
            
            from .sstable import SSTableBuilder
            
            # 创建SSTable构建器
            builder = SSTableBuilder(new_file_path)
            
            # 添加所有合并后的键值对
            for key, value in sorted(merged_data.items()):
                builder.add(key, value)
            
            # 完成构建并写入文件
            builder.finish()
            
            # 获取新文件大小
            file_size = os.path.getsize(new_file_path)
            
            # 创建新文件的元数据
            new_file_meta = FileMetaData(
                file_number=new_file_number,
                file_size=file_size,
                smallest_key=smallest_key,
                largest_key=largest_key,
                level=output_level
            )
            
            # 将新文件添加到版本编辑
            self.edit.add_file(output_level, new_file_meta)
            
            # 应用版本编辑
            success = self.version_set.apply_version_edit(self.edit)
            
            if success:
                print(f"压缩完成，新文件: {new_file_number}.sst (层级 {output_level})")
            else:
                print("应用版本编辑失败")
                # 清理创建的文件
                if os.path.exists(new_file_path):
                    os.remove(new_file_path)
            
            return success
        except Exception as e:
            print(f"压缩操作失败: {e}")
            return False 