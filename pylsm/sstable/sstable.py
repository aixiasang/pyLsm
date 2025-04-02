"""
SSTable模块

该模块实现了SSTable（Sorted String Table）文件的创建、读取和查询。
SSTable是一种不可变的、有序的键值对集合，存储在磁盘上。

文件格式：
+---------------+---------------+---------------+---------------+---------------+---------------+
| 数据块1       | 数据块2       | ...           | 元数据块      | 索引块        | 页脚          |
+---------------+---------------+---------------+---------------+---------------+---------------+

页脚格式：
+---------------+---------------+---------------+---------------+---------------+
| 元数据块偏移  | 索引块偏移    | 布隆过滤器偏移 | 魔数         | CRC校验码     |
| uint64        | uint64        | uint64        | 8字节        | uint32        |
+---------------+---------------+---------------+---------------+---------------+
"""
import os
import io
import time
import struct
import zlib
from typing import Dict, List, Tuple, Optional, Iterator, BinaryIO

from pylsm.utils import encode_key, decode_key, encode_value, decode_value
from pylsm.sstable.block import BlockBuilder, BlockIterator, CompressionType, build_data_block
from pylsm.memtable import MemTable, MemTableEntry, EntryType
from pylsm.bloom_filter import BloomFilter


# SSTable魔数，用于文件格式识别
SSTABLE_MAGIC = b"PyLSMDB1"


class SSTableBuilder:
    """
    SSTable构建器，用于将内存表写入磁盘。
    """
    
    def __init__(self, filename: str, block_size: int = 4096, 
                 compression_type: CompressionType = CompressionType.NONE,
                 enable_bloom_filter: bool = True, bits_per_key: int = 10):
        """
        初始化SSTable构建器。
        
        Args:
            filename: SSTable文件路径
            block_size: 数据块大小（字节）
            compression_type: 压缩类型
            enable_bloom_filter: 是否启用布隆过滤器
            bits_per_key: 布隆过滤器中每个键使用的位数
        """
        self.filename = filename
        self.block_size = block_size
        self.compression_type = compression_type
        self.enable_bloom_filter = enable_bloom_filter
        self.bits_per_key = bits_per_key
        
        # 打开文件
        self.file = open(filename, 'wb')
        
        # 当前正在构建的数据块
        self.data_block_builder = BlockBuilder(block_size, 16, compression_type)
        
        # 已完成的数据块偏移列表 [(最大键, 偏移, 大小), ...]
        self.data_blocks: List[Tuple[bytes, int, int]] = []
        
        # 元数据
        self.num_entries = 0
        self.smallest_key: Optional[bytes] = None
        self.largest_key: Optional[bytes] = None
        
        # 布隆过滤器
        self.bloom_filter = BloomFilter(bits_per_key) if enable_bloom_filter else None
        
        # 文件偏移
        self.offset = 0
    
    def add(self, key: bytes, value: bytes) -> None:
        """
        添加键值对到SSTable。
        
        Args:
            key: 键（字节）
            value: 值（字节）
        """
        # 更新布隆过滤器
        if self.bloom_filter:
            self.bloom_filter.add(key)
        
        # 更新键范围
        if self.smallest_key is None or key < self.smallest_key:
            self.smallest_key = key
        if self.largest_key is None or key > self.largest_key:
            self.largest_key = key
        
        # 尝试添加到当前数据块
        if not self.data_block_builder.add(key, value):
            # 如果数据块已满，完成当前块并开始新块
            self._finish_data_block()
            # 添加到新块
            self.data_block_builder.add(key, value)
        
        self.num_entries += 1
    
    def _finish_data_block(self) -> None:
        """完成当前数据块并将其写入文件。"""
        if self.data_block_builder.num_entries == 0:
            return
        
        # 获取块数据
        block_data = self.data_block_builder.finish()
        
        # 记录块信息：最大键、块偏移、块大小
        self.data_blocks.append((
            self.data_block_builder.last_key,
            self.offset,
            len(block_data)
        ))
        
        # 写入文件
        self.file.write(block_data)
        self.offset += len(block_data)
        
        # 重置块构建器
        self.data_block_builder.reset()
    
    def _write_index_block(self) -> int:
        """
        写入索引块。
        
        索引块包含所有数据块的元数据：
        - 数据块的最大键
        - 数据块的文件偏移
        - 数据块的大小
        
        Returns:
            索引块的起始偏移
        """
        index_block_offset = self.offset
        
        # 创建索引块
        index_block_builder = BlockBuilder(
            block_size=self.block_size,
            restart_interval=1,  # 索引条目较少，使用较小的重启点间隔
            compression_type=self.compression_type
        )
        
        # 向索引块添加每个数据块的元数据
        for largest_key, block_offset, block_size in self.data_blocks:
            # 索引块的键是数据块的最大键
            # 索引块的值是数据块的偏移和大小
            value = struct.pack("<QQ", block_offset, block_size)
            index_block_builder.add(largest_key, value)
        
        # 完成并写入索引块
        index_block_data = index_block_builder.finish()
        self.file.write(index_block_data)
        
        # 更新偏移
        self.offset += len(index_block_data)
        
        return index_block_offset
    
    def _write_bloom_filter(self) -> int:
        """
        写入布隆过滤器。
        
        Returns:
            布隆过滤器的起始偏移，如果未启用布隆过滤器则为0
        """
        if not self.bloom_filter:
            return 0
        
        bloom_filter_offset = self.offset
        
        # 获取布隆过滤器数据
        bloom_data = self.bloom_filter.to_bytes()
        
        # 写入布隆过滤器数据
        self.file.write(bloom_data)
        
        # 更新偏移
        self.offset += len(bloom_data)
        
        return bloom_filter_offset
    
    def _write_metadata_block(self) -> int:
        """
        写入元数据块。
        
        元数据块包含有关SSTable文件的全局信息：
        - 条目数量
        - 最小键
        - 最大键
        - 创建时间
        - 布隆过滤器参数
        
        Returns:
            元数据块的起始偏移
        """
        metadata_offset = self.offset
        
        # 创建元数据块
        metadata = {
            'num_entries': self.num_entries,
            'smallest_key': self.smallest_key,
            'largest_key': self.largest_key,
            'creation_time': int(time.time()),
            'bloom_filter_enabled': self.enable_bloom_filter,
            'bloom_filter_bits_per_key': self.bits_per_key if self.enable_bloom_filter else 0
        }
        
        # 序列化元数据
        metadata_serialized = str(metadata).encode('utf-8')
        
        # 写入元数据大小和内容
        size_bytes = struct.pack("<Q", len(metadata_serialized))
        self.file.write(size_bytes)
        self.file.write(metadata_serialized)
        
        # 更新偏移
        self.offset += len(size_bytes) + len(metadata_serialized)
        
        return metadata_offset
    
    def _write_footer(self, metadata_offset: int, index_offset: int, 
                     bloom_filter_offset: int) -> None:
        """
        写入文件页脚。
        
        Args:
            metadata_offset: 元数据块偏移
            index_offset: 索引块偏移
            bloom_filter_offset: 布隆过滤器偏移
        """
        # 页脚格式：元数据偏移(8B) | 索引偏移(8B) | 布隆过滤器偏移(8B) | 魔数(8B) | CRC(4B)
        footer = struct.pack("<QQQ", metadata_offset, index_offset, bloom_filter_offset)
        footer += SSTABLE_MAGIC
        
        # 计算CRC
        crc = zlib.crc32(footer)
        footer += struct.pack("<I", crc)
        
        # 写入页脚
        self.file.write(footer)
    
    def finish(self) -> Tuple[bytes, bytes]:
        """
        完成SSTable构建并返回文件的键范围。
        
        Returns:
            (最小键, 最大键)元组
        """
        # 确保最后一个数据块已完成
        self._finish_data_block()
        
        # 写入元数据块
        metadata_offset = self._write_metadata_block()
        
        # 写入布隆过滤器
        bloom_filter_offset = self._write_bloom_filter()
        
        # 写入索引块
        index_offset = self._write_index_block()
        
        # 写入页脚
        self._write_footer(metadata_offset, index_offset, bloom_filter_offset)
        
        # 刷新并关闭文件
        self.file.flush()
        self.file.close()
        
        return self.smallest_key, self.largest_key
    
    @classmethod
    def build_from_memtable(cls, memtable: MemTable, filename: str, block_size: int = 4096,
                           compression_type: CompressionType = CompressionType.NONE,
                           enable_bloom_filter: bool = True, bits_per_key: int = 10) -> Tuple[bytes, bytes]:
        """
        从内存表构建SSTable。
        
        Args:
            memtable: 内存表
            filename: SSTable文件路径
            block_size: 数据块大小
            compression_type: 压缩类型
            enable_bloom_filter: 是否启用布隆过滤器
            bits_per_key: 布隆过滤器中每个键使用的位数
            
        Returns:
            (最小键, 最大键)元组
        """
        builder = cls(filename, block_size, compression_type, enable_bloom_filter, bits_per_key)
        
        # 遍历内存表中的所有条目
        for entry in memtable.iteritems():
            # 跳过删除标记
            if entry.entry_type == EntryType.DELETE:
                # 为删除标记添加一个特殊值
                builder.add(entry.key, b"__DELETED__")
            else:
                builder.add(entry.key, entry.value)
        
        return builder.finish()


class SSTableIterator:
    """
    SSTable迭代器，用于遍历SSTable文件中的键值对。
    """
    
    def __init__(self, sstable: 'SSTable'):
        """
        初始化SSTable迭代器。
        
        Args:
            sstable: SSTable对象
        """
        self.sstable = sstable
        self.current_block_index = -1
        self.current_block_iterator = None
    
    def seek_to_first(self) -> None:
        """将迭代器移动到第一个键值对。"""
        # 如果没有数据块，迭代器无效
        if not self.sstable.index_entries:
            return
        
        # 移动到第一个数据块
        self.current_block_index = 0
        self._load_current_block()
        self.current_block_iterator.seek_to_first()
    
    def seek_to_last(self) -> None:
        """将迭代器移动到最后一个键值对。"""
        # 如果没有数据块，迭代器无效
        if not self.sstable.index_entries:
            return
        
        # 移动到最后一个数据块
        self.current_block_index = len(self.sstable.index_entries) - 1
        self._load_current_block()
        self.current_block_iterator.seek_to_last()
    
    def seek(self, key: bytes) -> None:
        """
        将迭代器移动到大于等于给定键的第一个键值对。
        
        Args:
            key: 目标键
        """
        # 如果没有数据块，迭代器无效
        if not self.sstable.index_entries:
            return
        
        # 使用二分查找找到适当的数据块
        left = 0
        right = len(self.sstable.index_entries) - 1
        
        while left <= right:
            mid = (left + right) // 2
            block_key = self.sstable.index_entries[mid][0]
            
            if block_key < key:
                left = mid + 1
            else:
                right = mid - 1
        
        # 移动到找到的数据块
        self.current_block_index = left if left < len(self.sstable.index_entries) else right
        if self.current_block_index < 0:
            self.current_block_index = 0
        
        # 加载数据块并查找键
        self._load_current_block()
        self.current_block_iterator.seek(key)
        
        # 如果当前块中没有找到，尝试下一个块
        if not self.current_block_iterator.valid() and self.current_block_index < len(self.sstable.index_entries) - 1:
            self.current_block_index += 1
            self._load_current_block()
            self.current_block_iterator.seek_to_first()
    
    def _load_current_block(self) -> None:
        """加载当前数据块。"""
        if self.current_block_index < 0 or self.current_block_index >= len(self.sstable.index_entries):
            self.current_block_iterator = None
            return
        
        # 获取块偏移和大小
        block_offset = self.sstable.index_entries[self.current_block_index][1]
        block_size = self.sstable.index_entries[self.current_block_index][2]
        
        # 从文件中读取块数据
        self.sstable.file.seek(block_offset)
        block_data = self.sstable.file.read(block_size)
        
        # 创建块迭代器
        self.current_block_iterator = BlockIterator(block_data)
    
    def valid(self) -> bool:
        """
        检查迭代器是否有效。
        
        Returns:
            如果迭代器指向一个有效的键值对则返回True
        """
        return (self.current_block_iterator is not None and 
                self.current_block_iterator.valid())
    
    def key(self) -> bytes:
        """
        获取当前键。
        
        Returns:
            当前键
            
        Raises:
            ValueError: 如果迭代器无效
        """
        if not self.valid():
            raise ValueError("Iterator is not valid")
        return self.current_block_iterator.key()
    
    def value(self) -> bytes:
        """
        获取当前值。
        
        Returns:
            当前值
            
        Raises:
            ValueError: 如果迭代器无效
        """
        if not self.valid():
            raise ValueError("Iterator is not valid")
        return self.current_block_iterator.value()
    
    def next(self) -> None:
        """移动到下一个键值对。"""
        if not self.valid():
            return
        
        # 移动块迭代器
        self.current_block_iterator.next()
        
        # 如果当前块已经遍历完毕，移动到下一个块
        if not self.current_block_iterator.valid():
            self.current_block_index += 1
            if self.current_block_index < len(self.sstable.index_entries):
                self._load_current_block()
                self.current_block_iterator.seek_to_first()
    
    def prev(self) -> None:
        """移动到前一个键值对。"""
        if not self.valid():
            return
        
        # 移动块迭代器
        self.current_block_iterator.prev()
        
        # 如果当前块已经遍历完毕，移动到上一个块
        if not self.current_block_iterator.valid():
            self.current_block_index -= 1
            if self.current_block_index >= 0:
                self._load_current_block()
                self.current_block_iterator.seek_to_last()


class SSTable:
    """
    SSTable（Sorted String Table）实现。
    
    SSTable是一种不可变的、有序的键值对集合，存储在磁盘上。
    """
    
    def __init__(self, filename: str):
        """
        初始化SSTable。
        
        Args:
            filename: SSTable文件路径
        """
        self.filename = filename
        self.file = open(filename, 'rb')
        
        # 读取页脚
        self._read_footer()
        
        # 读取元数据
        self._read_metadata()
        
        # 读取索引
        self._read_index()
        
        # 读取布隆过滤器（如果存在）
        self._read_bloom_filter()
    
    def _read_footer(self) -> None:
        """读取SSTable文件页脚。"""
        # 页脚大小：元数据偏移(8B) + 索引偏移(8B) + 布隆过滤器偏移(8B) + 魔数(8B) + CRC(4B)
        footer_size = 8 + 8 + 8 + 8 + 4
        
        # 移动到页脚开始处
        self.file.seek(-footer_size, os.SEEK_END)
        
        # 读取页脚
        footer = self.file.read(footer_size)
        
        # 验证魔数
        magic = footer[24:32]
        if magic != SSTABLE_MAGIC:
            raise ValueError(f"无效的SSTable文件：魔数不匹配 {magic} != {SSTABLE_MAGIC}")
        
        # 验证CRC
        crc_stored = struct.unpack("<I", footer[32:36])[0]
        crc_computed = zlib.crc32(footer[:32])
        if crc_stored != crc_computed:
            raise ValueError("SSTable文件已损坏：CRC校验失败")
        
        # 解析偏移
        self.metadata_offset, self.index_offset, self.bloom_filter_offset = struct.unpack("<QQQ", footer[:24])
    
    def _read_metadata(self) -> None:
        """读取SSTable元数据块。"""
        # 移动到元数据块开始处
        self.file.seek(self.metadata_offset)
        
        # 读取元数据大小
        size_bytes = self.file.read(8)
        metadata_size = struct.unpack("<Q", size_bytes)[0]
        
        # 读取元数据内容
        metadata_serialized = self.file.read(metadata_size)
        
        # 解析元数据
        # 这里使用了一个简单的字符串表示，实际应用中可能需要更健壮的序列化
        metadata_str = metadata_serialized.decode('utf-8')
        metadata_dict = eval(metadata_str)  # 注意：这在实际应用中不安全
        
        # 存储元数据
        self.num_entries = metadata_dict['num_entries']
        self.smallest_key = metadata_dict['smallest_key']
        self.largest_key = metadata_dict['largest_key']
        self.creation_time = metadata_dict['creation_time']
        self.bloom_filter_enabled = metadata_dict['bloom_filter_enabled']
        self.bloom_filter_bits_per_key = metadata_dict['bloom_filter_bits_per_key']
    
    def _read_index(self) -> None:
        """读取SSTable索引块。"""
        # 移动到索引块开始处
        self.file.seek(self.index_offset)
        
        # 读取索引块直到元数据块开始
        index_size = self.metadata_offset - self.index_offset
        index_data = self.file.read(index_size)
        
        # 创建索引块迭代器
        index_iterator = BlockIterator(index_data)
        
        # 解析索引条目
        self.index_entries = []
        index_iterator.seek_to_first()
        while index_iterator.valid():
            key = index_iterator.key()
            value = index_iterator.value()
            offset, size = struct.unpack("<QQ", value)
            self.index_entries.append((key, offset, size))
            index_iterator.next()
    
    def _read_bloom_filter(self) -> None:
        """读取SSTable布隆过滤器（如果存在）。"""
        if not self.bloom_filter_enabled or self.bloom_filter_offset == 0:
            self.bloom_filter = None
            return
        
        # 移动到布隆过滤器开始处
        self.file.seek(self.bloom_filter_offset)
        
        # 读取布隆过滤器数据
        bloom_size = self.index_offset - self.bloom_filter_offset
        bloom_data = self.file.read(bloom_size)
        
        # 创建布隆过滤器
        self.bloom_filter = BloomFilter.from_bytes(bloom_data)
    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        获取键对应的值。
        
        Args:
            key: 键（字节）
            
        Returns:
            如果键存在则返回值，否则返回None
        """
        # 检查键是否在SSTable的范围内
        if key < self.smallest_key or key > self.largest_key:
            return None
        
        # 如果启用了布隆过滤器，先检查键是否可能存在
        if self.bloom_filter and not self.bloom_filter.may_contain(key):
            return None
        
        # 创建迭代器并查找键
        iterator = SSTableIterator(self)
        iterator.seek(key)
        
        # 如果找到了键，返回值
        if iterator.valid() and iterator.key() == key:
            value = iterator.value()
            # 检查是否是删除标记
            if value == b"__DELETED__":
                return None
            return value
        
        return None
    
    def iterator(self) -> SSTableIterator:
        """
        获取SSTable迭代器。
        
        Returns:
            SSTable迭代器
        """
        return SSTableIterator(self)
    
    def close(self) -> None:
        """关闭SSTable文件。"""
        if self.file:
            self.file.close()
            self.file = None 