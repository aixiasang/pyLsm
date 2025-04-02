"""
PyLSM数据库主模块。

本模块实现了基于LSM树（Log-Structured Merge Tree）的键值存储引擎。
主要特点包括：
1. 写入优先：使用MemTable加速写操作
2. 分层存储：SSTable文件按层级组织，减少空间放大
3. 布隆过滤器：加速查询操作，减少不必要的磁盘IO
4. 数据压缩：定期合并小文件，优化存储和查询效率
"""
import os
import threading
import time
from typing import Dict, Optional, Iterator, Tuple, List, Set, Any, Union

from .memtable import MemTable
from .wal import WAL
from .sstable import SSTable, SSTableBuilder
from .version_set import VersionSet, FileMetaData, Compaction, Version, LEVEL_NUMBER
from .config import Config, default_config
from .bloom_filter import BloomFilter


class DB:
    """
    LSM树实现的键值数据库。
    
    特点：
    - 数据先写入内存表(MemTable)，再刷写到磁盘上的SSTable文件
    - 使用WAL(预写日志)确保数据持久性
    - 采用分层合并策略减少磁盘IO和空间放大
    """
    
    def __init__(self, db_path: str, config: Optional[Config] = None):
        """
        初始化数据库。
        
        Args:
            db_path: 数据库目录路径
            config: 数据库配置，如果为None则使用默认配置
        """
        self.db_path = db_path
        self.config = config or default_config()
        self.memtable = MemTable()
        self.version_set = VersionSet(db_path)
        self.wal = WAL(os.path.join(db_path, "wal"))
        self._lock = threading.RLock()
        
        # 确保数据库目录存在
        os.makedirs(db_path, exist_ok=True)
        
        # 恢复数据库状态
        self._recover()
        
        # 初始化计数器
        self.write_count = 0
    
    def _recover(self):
        """从磁盘恢复数据库状态"""
        # 恢复版本集状态
        if not self.version_set.recover():
            raise RuntimeError(f"无法恢复数据库版本状态: {self.db_path}")
        
        # 从WAL恢复数据（如果存在）
        try:
            for key, value in self.wal.read_all():
                self.memtable.put(key, value)
        except Exception as e:
            print(f"从WAL恢复失败: {e}")
    
    def put(self, key: Union[str, bytes], value: Union[str, bytes]) -> None:
        """
        存储键值对。
        
        Args:
            key: 键（字符串或字节）
            value: 值（字符串或字节）
        """
        with self._lock:
            # 转换为字节
            key_bytes = key.encode('utf-8') if isinstance(key, str) else key
            value_bytes = value.encode('utf-8') if isinstance(value, str) else value
            
            # 写入WAL
            self.wal.append(key_bytes, value_bytes)
            
            # 写入内存表
            self.memtable.put(key_bytes, value_bytes)
            
            # 如果内存表大小超过阈值，将其刷写到磁盘
            if self.memtable.size() >= self.config.memtable_size_threshold:
                self._flush_memtable()
            
            # 增加写入计数，检查是否需要执行压缩
            self.write_count += 1
            if self.config.enable_automatic_compaction and self.write_count % self.config.compaction_check_interval == 0:
                self._maybe_compact()
    
    def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """
        获取键对应的值。
        
        Args:
            key: 键（字符串或字节）
            
        Returns:
            如果找到键则返回对应的值，否则返回None
        """
        with self._lock:
            # 转换为字节
            key_bytes = key.encode('utf-8') if isinstance(key, str) else key
            
            # 首先查找内存表
            value = self.memtable.get(key_bytes)
            if value is not None:
                # 检查是否为墓碑值
                if value == b'':
                    return None
                return value
            
            # 然后查找SSTable文件
            return self._get_from_sstables(key_bytes)
    
    def delete(self, key: Union[str, bytes]) -> None:
        """
        删除键值对。实际上是写入一个墓碑值。
        
        Args:
            key: 要删除的键
        """
        self.put(key, b'')  # 空字节串作为墓碑值
    
    def _get_from_sstables(self, key: bytes) -> Optional[bytes]:
        """
        从SSTable中获取键值。
        
        参数：
            key: 键
            
        返回：
            与键关联的值，如果不存在则返回None
        """
        version = self.version_set.get_current()
        
        # 从每层SSTable中查找键
        for level in range(LEVEL_NUMBER):
            # 对于level > 0的层级，文件是有序的，使用二分查找
            if level > 0:
                # 二分查找找到可能包含键的文件
                files = version.files[level]
                if not files:
                    continue
                    
                left, right = 0, len(files) - 1
                target_file = None
                
                while left <= right:
                    mid = (left + right) // 2
                    file_meta = files[mid]
                    
                    if key < file_meta.smallest_key:
                        right = mid - 1
                    elif key > file_meta.largest_key:
                        left = mid + 1
                    else:
                        target_file = file_meta
                        break
                
                if target_file:
                    # 尝试从文件中获取键
                    try:
                        sstable = SSTable(self._get_table_path(target_file.file_number))
                        value = sstable.get(key)
                        if value is not None:
                            return value
                    except Exception as e:
                        print(f"从SSTable获取键失败: {e}")
                        continue
            else:
                # Level 0的文件可能有重叠，需要从最新到最旧的顺序检查所有文件
                for file_meta in reversed(version.files[level]):
                    if key >= file_meta.smallest_key and key <= file_meta.largest_key:
                        try:
                            sstable = SSTable(self._get_table_path(file_meta.file_number))
                            value = sstable.get(key)
                            if value is not None:
                                return value
                        except Exception as e:
                            print(f"从SSTable获取键失败: {e}")
                            continue
        
        return None
    
    def range(self, start_key: Optional[Union[str, bytes]] = None, end_key: Optional[Union[str, bytes]] = None) -> Iterator[Tuple[str, bytes]]:
        """
        获取指定范围内的键值对。
        
        参数：
            start_key: 起始键，如果为None则从最小键开始，可以是字符串或字节
            end_key: 结束键，如果为None则到最大键结束，可以是字符串或字节
            
        返回：
            范围内的键值对迭代器
        """
        # 转换键为字节
        if start_key is None:
            start_bytes = b''
        elif isinstance(start_key, str):
            start_bytes = start_key.encode('utf-8')
        else:
            start_bytes = start_key
            
        if end_key is None:
            end_bytes = b'\xff' * 100
        elif isinstance(end_key, str):
            end_bytes = end_key.encode('utf-8')
        else:
            end_bytes = end_key
        
        # 从内存表获取范围数据
        mem_results = {}
        
        for key, value in self.memtable.range(start_bytes, end_bytes):
            mem_results[key] = value
        
        # 从SSTable获取范围数据
        sst_results = self._scan_sstables(start_bytes, end_bytes)
        
        # 合并结果，内存表数据优先
        all_results = {}
        for key, value in sst_results.items():
            if key not in mem_results:
                all_results[key] = value
        
        # 添加内存表数据
        for key, value in mem_results.items():
            all_results[key] = value
        
        # 过滤已删除的键和墓碑值
        filtered_results = {k: v for k, v in all_results.items() if v and v != b''}
        
        # 按键排序并返回
        for key in sorted(filtered_results.keys()):
            yield key.decode('utf-8'), filtered_results[key]
    
    # 添加items方法作为range的别名
    def items(self) -> Iterator[Tuple[bytes, bytes]]:
        """
        获取数据库中所有键值对的迭代器。
        
        Returns:
            所有键值对的迭代器
        """
        return self.range()
    
    def _scan_sstables(self, start_key: bytes, end_key: bytes) -> Dict[bytes, bytes]:
        """
        从SSTable中扫描指定范围的键值对。
        
        参数：
            start_key: 起始键
            end_key: 结束键
            
        返回：
            范围内的键值对
        """
        results = {}
        version = self.version_set.get_current()
        
        # 从最高层级往下扫描，确保更新的数据覆盖旧数据
        for level in range(LEVEL_NUMBER - 1, -1, -1):
            for file_meta in reversed(version.files[level]):
                # 检查文件的键范围是否与查询范围有重叠
                if not (file_meta.largest_key < start_key or file_meta.smallest_key > end_key):
                    try:
                        sstable = SSTable(self._get_table_path(file_meta.file_number))
                        for key, value in sstable.get_range(start_key, end_key):
                            if key not in results:  # 避免覆盖更高层级的数据
                                results[key] = value
                    except Exception as e:
                        print(f"从SSTable获取范围失败: {e}")
                        continue
        
        return results
    
    def _flush_memtable(self) -> None:
        """将内存表写入SSTable文件"""
        if self.memtable.is_empty():
            return
        
        # 获取新文件编号
        file_number = self.version_set.get_next_file_number()
        file_path = os.path.join(self.db_path, f"{file_number}.sst")
        
        # 创建SSTable构建器
        builder = SSTableBuilder(file_path)
        
        # 添加所有键值对
        for key, value in self.memtable.items():
            builder.add(key, value)
        
        # 完成构建
        builder.finish()
        
        # 获取文件大小
        file_size = os.path.getsize(file_path)
        
        # 获取键范围
        items = list(self.memtable.items())
        if not items:
            # 内存表为空，不创建SSTable
            os.remove(file_path)
            return
        
        smallest_key = min(key for key, _ in items)
        largest_key = max(key for key, _ in items)
        
        # 创建文件元数据（新文件添加到Level 0）
        file_meta = FileMetaData(
            file_number=file_number,
            file_size=file_size,
            smallest_key=smallest_key,
            largest_key=largest_key,
            level=0  # Level 0
        )
        
        # 更新版本集
        from .version_set import VersionEdit
        edit = VersionEdit()
        edit.add_file(0, file_meta)
        self.version_set.apply_version_edit(edit)
        
        # 先关闭当前WAL
        old_wal = self.wal
        if hasattr(old_wal, 'close'):
            old_wal.close()
        
        # 创建新的内存表
        self.memtable = MemTable()
        
        # 创建新的WAL文件
        wal_path = os.path.join(self.db_path, "wal")
        
        # 给足够的时间让系统释放文件句柄
        for attempt in range(3):
            try:
                # 尝试重命名旧WAL文件
                if os.path.exists(wal_path):
                    archive_path = f"{wal_path}.{int(time.time())}"
                    os.rename(wal_path, archive_path)
                break
            except (OSError, PermissionError) as e:
                if attempt < 2:
                    print(f"重命名旧WAL文件失败，重试中: {e}")
                    time.sleep(0.1 * (attempt + 1))  # 渐进式延迟
                else:
                    print(f"警告：无法重命名旧WAL文件: {e}")
        
        # 创建新的WAL实例
        self.wal = WAL(wal_path, self.config)
    
    def _maybe_compact(self) -> None:
        """检查是否需要进行压缩，并在需要时执行"""
        if not self.config.enable_automatic_compaction:
            return
        
        if not self.version_set.needs_compaction():
            return
        
        # 选择要压缩的文件
        level, input_files_level_n, input_files_level_n_plus_1 = self.version_set.pick_compaction_files()
        
        if level == -1 or (not input_files_level_n and not input_files_level_n_plus_1):
            return
        
        # 创建压缩任务
        compaction = Compaction(
            version_set=self.version_set,
            level=level,
            input_files_level_n=input_files_level_n,
            input_files_level_n_plus_1=input_files_level_n_plus_1
        )
        
        # 执行压缩
        compaction.compact()
    
    def compact(self) -> None:
        """手动触发压缩操作"""
        with self._lock:
            self._maybe_compact()
            
    def flush(self) -> None:
        """
        强制刷写内存表到磁盘，确保所有数据持久化。
        """
        with self._lock:
            if not self.memtable.is_empty():
                self._flush_memtable()
    
    def close(self) -> None:
        """关闭数据库，释放所有资源。"""
        with self._lock:
            try:
                # 刷写内存表
                if not self.memtable.is_empty():
                    self._flush_memtable()
            except Exception as e:
                print(f"刷写内存表时出错: {e}")
            
            # 关闭WAL
            if hasattr(self, 'wal') and self.wal:
                try:
                    self.wal.close()
                except Exception as e:
                    print(f"关闭WAL时出错: {e}")
            
            # 关闭版本集
            if hasattr(self, 'version_set') and self.version_set:
                try:
                    self.version_set.close()
                except Exception as e:
                    print(f"关闭版本集时出错: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _get_table_path(self, file_number: int) -> str:
        """
        获取SSTable文件的路径。
        
        参数：
            file_number: 文件编号
            
        返回：
            SSTable文件的路径
        """
        return os.path.join(self.db_path, f"{file_number}.sst")