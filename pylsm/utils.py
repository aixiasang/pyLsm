"""
工具函数模块

包含编解码函数和其他辅助功能。
"""
from typing import Optional, Dict, List, Tuple, Any, Iterator
import os
import re
import shutil
import struct
import logging
import sys


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('pylsm')


def encode_key(key):
    """
    将键编码为字节。
    检查输入类型，如果已经是字节则直接返回，否则进行编码。
    
    Args:
        key: 要编码的键（字符串或字节）
        
    Returns:
        bytes: 编码后的键
    """
    if isinstance(key, bytes):
        return key
    elif isinstance(key, str):
        return key.encode('utf-8')
    else:
        raise TypeError(f"Key must be str or bytes, got {type(key)}")


def decode_key(key_bytes):
    """
    将字节解码为键。
    
    Args:
        key_bytes: 要解码的键字节
        
    Returns:
        str: 解码后的键
    """
    return key_bytes.decode('utf-8')


def encode_value(value):
    """
    将值编码为字节。
    检查输入类型，如果已经是字节则直接返回，否则进行编码。
    
    Args:
        value: 要编码的值（字符串或字节）
        
    Returns:
        bytes: 编码后的值
    """
    if isinstance(value, bytes):
        return value
    elif isinstance(value, str):
        return value.encode('utf-8')
    else:
        raise TypeError(f"Value must be str or bytes, got {type(value)}")


def decode_value(value_bytes):
    """
    将字节解码为值。
    
    Args:
        value_bytes: 要解码的值字节
        
    Returns:
        str: 解码后的值
    """
    return value_bytes.decode('utf-8')


def ensure_dir_exists(path: str) -> None:
    """
    确保目录存在，如果不存在则创建。
    
    Args:
        path: 目录路径
    """
    os.makedirs(path, exist_ok=True)


def list_files_with_suffix(dir_path: str, suffix: str) -> List[str]:
    """
    列出指定目录中具有指定后缀的所有文件。
    
    Args:
        dir_path: 目录路径
        suffix: 文件后缀
        
    Returns:
        文件路径列表
    """
    if not os.path.exists(dir_path):
        return []
    
    files = []
    for filename in os.listdir(dir_path):
        if filename.endswith(suffix):
            files.append(os.path.join(dir_path, filename))
    
    return sorted(files)


def parse_file_number(filename: str) -> int:
    """
    从文件名中解析文件编号。
    
    Args:
        filename: 文件名，格式为"prefix_NUMBER.suffix"
        
    Returns:
        文件编号
        
    Raises:
        ValueError: 如果文件名格式无效
    """
    match = re.search(r'_(\d+)\.', filename)
    if not match:
        raise ValueError(f"无效的文件名格式: {filename}")
    
    return int(match.group(1))


def human_readable_size(size_bytes: int) -> str:
    """
    将字节大小转换为人类可读的格式。
    
    Args:
        size_bytes: 字节大小
        
    Returns:
        人类可读的大小字符串（如 "4.5 MB"）
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0 or unit == 'TB':
            break
        size_bytes /= 1024.0
    
    return f"{size_bytes:.2f} {unit}"


def varint_encode(n: int) -> bytes:
    """
    使用变长整数编码。
    
    Args:
        n: 整数
        
    Returns:
        编码后的字节
    """
    result = bytearray()
    while n >= 0x80:
        result.append((n & 0x7F) | 0x80)
        n >>= 7
    result.append(n & 0x7F)
    return bytes(result)


def varint_decode(data: bytes, start_pos: int = 0) -> Tuple[int, int]:
    """
    解码变长整数。
    
    Args:
        data: 编码的字节
        start_pos: 起始位置
        
    Returns:
        (整数值, 下一个位置)
    """
    result = 0
    shift = 0
    pos = start_pos
    
    while True:
        if pos >= len(data):
            raise ValueError("变长整数解码超出数据范围")
        
        b = data[pos]
        pos += 1
        
        result |= ((b & 0x7F) << shift)
        if not (b & 0x80):
            break
        
        shift += 7
        if shift > 63:
            raise ValueError("变长整数过大")
    
    return result, pos 