"""
WAL（写前日志）模块，用于确保数据持久性和崩溃恢复。
"""

import os
import struct
import pickle
import time
import threading
from typing import Optional, List, Tuple, Iterator

from .config import Config


class WAL:
    """
    写前日志实现，支持添加记录、读取记录和恢复操作。
    
    WAL文件格式：
    - 记录格式：[CRC(4字节) | 记录大小(4字节) | 类型(1字节) | 数据内容]
    - 类型：0=完整记录, 1=第一个分片, 2=中间分片, 3=最后一个分片
    """
    
    # 记录类型常量
    FULL = 0    # 完整记录
    FIRST = 1   # 第一个分片
    MIDDLE = 2  # 中间分片
    LAST = 3    # 最后一个分片
    
    # 头部大小（CRC + 记录大小 + 类型）
    HEADER_SIZE = 4 + 4 + 1
    
    def __init__(self, path: str, config=None):
        """
        初始化写前日志。
        
        Args:
            path: WAL文件路径。
            config: 配置对象，如果为None则使用默认配置。
        """
        self.path = path
        self.config = config if config is not None else Config()
        self.file = None
        self.mutex = threading.Lock()
        self.last_flush = time.time()
        
        # 默认WAL配置
        self.wal_flush_interval = 1.0  # 默认1秒
        self.wal_size_threshold = 4 * 1024 * 1024  # 默认4MB
        
        # 如果文件存在，打开进行追加，否则创建
        if os.path.exists(path):
            # 确保目录存在
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.file = open(path, 'ab+')
            # 移动到文件开头以便于读取
            self.file.seek(0)
        else:
            # 确保目录存在
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.file = open(path, 'wb+')
    
    def add_record(self, key: bytes, value: Optional[bytes]) -> None:
        """
        添加记录到WAL。
        
        Args:
            key: 键（字节串）。
            value: 值（字节串）或None（表示删除标记）。
        """
        # 序列化记录
        record = pickle.dumps((key, value))
        record_size = len(record)
        
        with self.mutex:
            # 计算块大小（可配置，默认为4KB）
            block_size = getattr(self.config, 'sstable_block_size', 4 * 1024)
            available = block_size - (self.file.tell() % block_size)
            
            # 如果剩余空间不足以存储至少头部，则填充到下一个块
            if available < self.HEADER_SIZE:
                # 填充零
                self.file.write(b'\x00' * available)
                available = block_size
            
            # 如果可用空间足够存储整个记录，写入一个完整记录
            if available >= self.HEADER_SIZE + record_size:
                self._write_physical_record(self.FULL, record)
            else:
                # 需要分片
                # 写入第一个分片
                first_part_size = available - self.HEADER_SIZE
                self._write_physical_record(self.FIRST, record[:first_part_size])
                
                # 写入中间分片
                record = record[first_part_size:]
                while len(record) > block_size - self.HEADER_SIZE:
                    part_size = block_size - self.HEADER_SIZE
                    self._write_physical_record(self.MIDDLE, record[:part_size])
                    record = record[part_size:]
                
                # 写入最后一个分片
                self._write_physical_record(self.LAST, record)
            
            # 检查是否需要刷新
            current_time = time.time()
            if (current_time - self.last_flush >= self.wal_flush_interval or
                self.file.tell() >= self.wal_size_threshold):
                self.file.flush()
                os.fsync(self.file.fileno())
                self.last_flush = current_time
    
    # 添加别名，与DB类保持兼容
    def append(self, key: bytes, value: Optional[bytes]) -> None:
        """
        append是add_record的别名，与DB类兼容。
        
        Args:
            key: 键（字节串）。
            value: 值（字节串）或None（表示删除标记）。
        """
        return self.add_record(key, value)
    
    def _write_physical_record(self, record_type: int, data: bytes) -> None:
        """
        写入物理记录到文件。
        
        Args:
            record_type: 记录类型（FULL, FIRST, MIDDLE, LAST）。
            data: 记录数据。
        """
        # 计算CRC
        crc = self._calculate_crc(data)
        
        # 写入头部
        self.file.write(struct.pack('!IIB', crc, len(data), record_type))
        
        # 写入数据
        self.file.write(data)
    
    def _calculate_crc(self, data: bytes) -> int:
        """
        计算数据的CRC校验和。
        
        Args:
            data: 要计算CRC的数据。
            
        Returns:
            CRC值。
        """
        # 这里使用简单的校验和，实际实现应使用更强的CRC32
        crc = 0
        for b in data:
            crc = (crc + b) & 0xFFFFFFFF
        return crc
    
    def read_all(self) -> Iterator[Tuple[bytes, Optional[bytes]]]:
        """
        读取WAL中的所有记录。
        
        Returns:
            键值对迭代器。
        """
        with self.mutex:
            # 保存当前位置
            current_pos = self.file.tell()
            
            # 移动到文件开头
            self.file.seek(0)
            
            # 读取所有记录
            records = []
            
            try:
                # 当前正在读取的分片记录
                fragments = []
                
                while True:
                    # 读取记录头部
                    header = self.file.read(self.HEADER_SIZE)
                    if not header or len(header) < self.HEADER_SIZE:
                        break
                    
                    # 解析头部
                    crc, length, record_type = struct.unpack('!IIB', header)
                    
                    # 读取数据
                    data = self.file.read(length)
                    if len(data) < length:
                        # 文件不完整
                        break
                    
                    # 验证CRC
                    computed_crc = self._calculate_crc(data)
                    if computed_crc != crc:
                        # CRC不匹配，跳过这条记录
                        continue
                    
                    # 处理不同类型的记录
                    if record_type == self.FULL:
                        # 完整记录
                        key, value = pickle.loads(data)
                        yield (key, value)
                    elif record_type == self.FIRST:
                        # 第一个分片
                        fragments = [data]
                    elif record_type == self.MIDDLE:
                        # 中间分片
                        fragments.append(data)
                    elif record_type == self.LAST:
                        # 最后一个分片
                        fragments.append(data)
                        # 合并所有分片并解析
                        complete_record = b''.join(fragments)
                        key, value = pickle.loads(complete_record)
                        yield (key, value)
                        fragments = []
            except Exception as e:
                # 发生异常，记录可能已损坏
                print(f"Error reading WAL: {e}")
            finally:
                # 恢复文件位置
                self.file.seek(current_pos)
    
    def close(self) -> None:
        """关闭WAL文件。"""
        if hasattr(self, 'file') and self.file:
            try:
                self.file.flush()
                self.file.close()
                self.file = None
            except Exception as e:
                print(f"关闭WAL文件时出错: {e}")
    
    def __del__(self) -> None:
        """析构函数，确保文件被关闭。"""
        self.close() 