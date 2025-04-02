"""
SSTable块模块

该模块实现了SSTable中的数据块，包括块的构建、序列化和查询功能。
"""
import io
import struct
import zlib
from enum import Enum
from typing import List, Tuple, Dict, Optional, Iterator, BinaryIO

from pylsm.utils import encode_key, decode_key, encode_value, decode_value, varint_encode, varint_decode


class CompressionType(Enum):
    """压缩类型枚举。"""
    NONE = 0
    SNAPPY = 1
    ZLIB = 2


class BlockBuilder:
    """
    SSTable数据块构建器。
    
    数据块格式如下：
    +-------------------+------------------------+------------------------+-------+------------------------+-------------------+
    | restart_interval  | num_restart_points     | restart_points         | data  | num_entries           | compression_type  |
    | varint            | varint                 | [uint32, ...]          | bytes | varint                | uint8             |
    +-------------------+------------------------+------------------------+-------+------------------------+-------------------+
    
    数据格式:
    +---------------+------------+---------------+---------------+
    | shared_prefix | key_suffix | value_length  | value         |
    | varint        | string     | varint        | string        |
    +---------------+------------+---------------+---------------+
    """
    
    def __init__(self, block_size: int = 4096, restart_interval: int = 16, 
                 compression_type: CompressionType = CompressionType.NONE):
        """
        初始化块构建器。
        
        Args:
            block_size: 目标块大小（字节）
            restart_interval: 重启点间隔（每隔几个键存储一个完整键）
            compression_type: 压缩类型
        """
        self.block_size = block_size
        self.restart_interval = restart_interval
        self.compression_type = compression_type
        
        # 数据缓冲区
        self.buffer = io.BytesIO()
        
        # 当前已添加的条目数
        self.num_entries = 0
        
        # 重启点位置列表
        self.restart_points: List[int] = []
        
        # 当前块大小的估计值
        self.estimated_block_size = 0
        
        # 上一个键，用于计算共享前缀
        self.last_key: Optional[bytes] = None
    
    def add(self, key: bytes, value: bytes) -> bool:
        """
        添加键值对到块。
        
        如果添加后块大小超过目标大小，返回False，否则返回True。
        
        Args:
            key: 键（字节）
            value: 值（字节）
            
        Returns:
            如果键值对成功添加则返回True，否则返回False
        """
        # 检查是否需要一个新的重启点
        if self.num_entries % self.restart_interval == 0:
            self.restart_points.append(self.buffer.tell())
            shared_prefix_length = 0  # 重启点处存储完整键
        else:
            # 计算与上一个键的共享前缀长度
            shared_prefix_length = 0
            if self.last_key is not None:
                min_len = min(len(key), len(self.last_key))
                for i in range(min_len):
                    if key[i] != self.last_key[i]:
                        break
                    shared_prefix_length += 1
        
        # 计算非共享部分
        key_suffix = key[shared_prefix_length:]
        
        # 写入数据：共享前缀长度、非共享部分长度、非共享部分、值长度、值
        self.buffer.write(varint_encode(shared_prefix_length))
        self.buffer.write(varint_encode(len(key_suffix)))
        self.buffer.write(key_suffix)
        self.buffer.write(varint_encode(len(value)))
        self.buffer.write(value)
        
        # 更新状态
        self.last_key = key
        self.num_entries += 1
        
        # 更新估计块大小
        # 数据大小 + 重启点数组大小 + 元数据大小
        self.estimated_block_size = (
            self.buffer.tell() +  # 数据大小
            len(self.restart_points) * 4 +  # 重启点数组大小
            20  # 元数据大小的估计值
        )
        
        # 检查块大小是否超出限制
        return self.estimated_block_size <= self.block_size
    
    def finish(self) -> bytes:
        """
        完成块的构建并返回序列化的块数据。
        
        Returns:
            序列化的块数据
        """
        # 获取数据部分
        data = self.buffer.getvalue()
        
        # 创建结果缓冲区
        result = io.BytesIO()
        
        # 写入重启点间隔
        result.write(varint_encode(self.restart_interval))
        
        # 写入重启点数量
        result.write(varint_encode(len(self.restart_points)))
        
        # 写入重启点位置
        for point in self.restart_points:
            result.write(struct.pack("<I", point))
        
        # 写入数据
        result.write(data)
        
        # 写入条目数
        result.write(varint_encode(self.num_entries))
        
        # 获取结果
        block_data = result.getvalue()
        
        # 如果需要压缩，执行压缩
        if self.compression_type == CompressionType.ZLIB:
            block_data = zlib.compress(block_data)
        
        # 创建最终结果缓冲区
        final_result = io.BytesIO()
        final_result.write(block_data)
        
        # 写入压缩类型
        final_result.write(bytes([self.compression_type.value]))
        
        return final_result.getvalue()
    
    def reset(self) -> None:
        """重置块构建器状态。"""
        self.buffer = io.BytesIO()
        self.num_entries = 0
        self.restart_points = []
        self.estimated_block_size = 0
        self.last_key = None


class BlockIterator:
    """
    SSTable数据块迭代器。
    """
    
    def __init__(self, block_data: bytes):
        """
        初始化块迭代器。
        
        Args:
            block_data: 块数据
        """
        self.block_data = block_data
        
        # 检查压缩类型并解压
        compression_type = CompressionType(block_data[-1])
        if compression_type == CompressionType.ZLIB:
            self.data = zlib.decompress(block_data[:-1])
        else:
            self.data = block_data[:-1]
        
        # 解析块结构
        self._parse_block_structure()
        
        # 当前位置
        self.current_entry_id = 0
        self.current_offset = 0
        self.current_key = b""
        
        # 初始化迭代器位置
        self.seek_to_first()
    
    def _parse_block_structure(self) -> None:
        """解析块结构。"""
        buffer = memoryview(self.data)
        pos = 0
        
        # 读取重启点间隔
        self.restart_interval, pos = varint_decode(buffer, pos)
        
        # 读取重启点数量
        self.num_restart_points, pos = varint_decode(buffer, pos)
        
        # 读取重启点位置
        self.restart_points = []
        for i in range(self.num_restart_points):
            point = struct.unpack("<I", buffer[pos:pos+4])[0]
            self.restart_points.append(point)
            pos += 4
        
        # 设置数据区域开始位置
        self.data_offset = pos
        
        # 找到条目数的位置（从末尾向前解析）
        self.footer_offset = len(buffer) - 1
        while self.footer_offset > 0 and buffer[self.footer_offset] >= 128:
            self.footer_offset -= 1
        
        # 解析条目数
        self.num_entries, _ = varint_decode(buffer, self.footer_offset)
        
        # 设置数据区域结束位置
        self.data_end = self.footer_offset
    
    def valid(self) -> bool:
        """
        检查迭代器是否有效。
        
        Returns:
            如果迭代器指向一个有效的位置则返回True
        """
        return 0 <= self.current_entry_id < self.num_entries
    
    def seek_to_first(self) -> None:
        """将迭代器移动到第一个条目。"""
        if self.num_entries == 0:
            self.current_entry_id = -1
            return
        
        self.current_entry_id = 0
        self.current_offset = self.restart_points[0]
        self.current_key = b""
        self._parse_current_entry()
    
    def seek_to_last(self) -> None:
        """将迭代器移动到最后一个条目。"""
        if self.num_entries == 0:
            self.current_entry_id = -1
            return
        
        # 找到最后一个重启点
        restart_idx = self.num_restart_points - 1
        self.current_offset = self.restart_points[restart_idx]
        self.current_key = b""
        
        # 跳到最后一个条目
        restart_entry_id = restart_idx * self.restart_interval
        
        # 从重启点开始，解析直到最后一个条目
        self.current_entry_id = restart_entry_id
        while self.current_entry_id < self.num_entries - 1:
            self._parse_current_entry()
            self.current_entry_id += 1
        
        # 解析最后一个条目
        self._parse_current_entry()
    
    def seek(self, target: bytes) -> None:
        """
        将迭代器移动到大于等于目标键的第一个条目。
        
        Args:
            target: 目标键
        """
        # 二分查找找到适当的重启点
        left = 0
        right = self.num_restart_points - 1
        
        while left <= right:
            mid = (left + right) // 2
            
            # 加载重启点的键
            restart_offset = self.restart_points[mid]
            restart_key = self._get_key_at_offset(restart_offset)
            
            if restart_key < target:
                left = mid + 1
            else:
                right = mid - 1
        
        # 如果没有找到适当的重启点，使用左边界
        restart_idx = left
        if restart_idx >= self.num_restart_points:
            # 如果目标键大于所有键，将迭代器设置为无效
            self.current_entry_id = self.num_entries
            return
        
        # 从找到的重启点开始线性查找
        self.current_offset = self.restart_points[restart_idx]
        self.current_key = b""
        self.current_entry_id = restart_idx * self.restart_interval
        
        # 线性搜索到第一个大于等于目标键的条目
        while self.current_entry_id < self.num_entries:
            self._parse_current_entry()
            if self.current_key >= target:
                break
            self.current_entry_id += 1
        
        # 检查是否越界
        if self.current_entry_id >= self.num_entries:
            self.current_entry_id = self.num_entries  # 设置为无效
    
    def _get_key_at_offset(self, offset: int) -> bytes:
        """
        获取指定偏移处的键。
        
        Args:
            offset: 数据偏移
            
        Returns:
            键值
        """
        data = self.data
        pos = offset
        
        # 重启点处存储的是完整键，没有共享前缀
        shared_prefix_length, pos = varint_decode(data, pos)
        assert shared_prefix_length == 0, "重启点必须存储完整键"
        
        key_suffix_length, pos = varint_decode(data, pos)
        key = data[pos:pos+key_suffix_length]
        
        return key
    
    def _parse_current_entry(self) -> None:
        """解析当前条目并更新当前键。"""
        if not self.valid():
            return
        
        data = self.data
        pos = self.current_offset
        
        # 读取共享前缀长度
        shared_prefix_length, pos = varint_decode(data, pos)
        
        # 读取键后缀
        key_suffix_length, pos = varint_decode(data, pos)
        key_suffix = data[pos:pos+key_suffix_length]
        pos += key_suffix_length
        
        # 构建完整键
        if shared_prefix_length == 0:
            self.current_key = key_suffix
        else:
            self.current_key = self.current_key[:shared_prefix_length] + key_suffix
        
        # 读取值长度
        value_length, pos = varint_decode(data, pos)
        
        # 跳过值部分
        pos += value_length
        
        # 更新当前偏移
        self.current_offset = pos
    
    def key(self) -> bytes:
        """
        获取当前键。
        
        Returns:
            当前键
        """
        if not self.valid():
            raise ValueError("迭代器无效")
        return self.current_key
    
    def value(self) -> bytes:
        """
        获取当前值。
        
        Returns:
            当前值
        """
        if not self.valid():
            raise ValueError("迭代器无效")
        
        data = self.data
        pos = self.current_offset
        
        # 回退以找到值的开始位置
        # 首先，找到键的共享前缀长度
        shared_prefix_length, pos = varint_decode(data, pos - self.current_offset)
        
        # 找到键后缀长度
        key_suffix_length, pos = varint_decode(data, pos)
        
        # 跳过键后缀
        pos += key_suffix_length
        
        # 读取值长度
        value_length, pos = varint_decode(data, pos)
        
        # 读取值
        value = data[pos:pos+value_length]
        
        return value
    
    def next(self) -> None:
        """移动到下一个条目。"""
        if not self.valid():
            return
        
        self.current_entry_id += 1
        if not self.valid():
            return
        
        # 如果遇到重启点，重置当前键
        if self.current_entry_id % self.restart_interval == 0:
            restart_idx = self.current_entry_id // self.restart_interval
            if restart_idx < self.num_restart_points:
                self.current_offset = self.restart_points[restart_idx]
                self.current_key = b""
        
        self._parse_current_entry()
    
    def prev(self) -> None:
        """移动到前一个条目。"""
        if not self.valid() or self.current_entry_id == 0:
            self.current_entry_id = -1  # 设置为无效
            return
        
        self.current_entry_id -= 1
        
        # 找到对应的重启点
        restart_idx = self.current_entry_id // self.restart_interval
        restart_entry_id = restart_idx * self.restart_interval
        self.current_offset = self.restart_points[restart_idx]
        self.current_key = b""
        
        # 从重启点开始，解析直到当前条目
        current_pos = self.current_entry_id - restart_entry_id
        for i in range(current_pos + 1):
            self._parse_current_entry()
            if i < current_pos:
                # 只在迭代过程中保存位置，最后一步实际设置条目
                pass


def build_data_block(key_values: List[Tuple[bytes, bytes]], block_size: int = 4096, 
                    restart_interval: int = 16, 
                    compression_type: CompressionType = CompressionType.NONE) -> bytes:
    """
    构建数据块。
    
    Args:
        key_values: 键值对列表
        block_size: 目标块大小
        restart_interval: 重启点间隔
        compression_type: 压缩类型
        
    Returns:
        序列化的块数据
    """
    builder = BlockBuilder(block_size, restart_interval, compression_type)
    
    for key, value in key_values:
        if not builder.add(key, value):
            # 如果块已满，强制添加最后一个键值对
            # 这可能会导致块略微超过目标大小
            break
    
    return builder.finish() 