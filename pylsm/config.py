"""
PyLSM配置模块。

该模块定义了数据库的可配置参数和默认值。
"""
import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional


@dataclass
class Config:
    """数据库配置类。"""
    
    # 目录配置
    data_dir: str = 'data'  # 数据目录
    wal_dir: str = 'wal'    # 预写日志目录
    
    # 内存表相关
    memtable_size_threshold: int = 4 * 1024 * 1024  # 默认4MB
    skiplist_max_level: int = 12  # 跳表最大层级
    skiplist_p: float = 0.5  # 跳表层级概率
    
    # SSTable相关
    sstable_block_size: int = 4 * 1024  # 数据块大小（默认4KB）
    max_file_size: int = 2 * 1024 * 1024  # 最大文件大小（默认2MB）
    
    # 布隆过滤器相关
    use_bloom_filter: bool = True  # 是否启用布隆过滤器
    bloom_filter_bits_per_key: int = 10  # 每个键的位数
    bloom_filter_hash_count: int = 7  # 哈希函数数量
    bloom_filter_false_positive_rate: float = 0.01  # 假阳性率
    
    # 压缩相关
    enable_automatic_compaction: bool = True  # 是否启用自动压缩
    compaction_trigger: int = 4  # 触发压缩的Level 0文件数
    compaction_max_level: int = 7  # 最大层级数
    compaction_level_size_multiplier: int = 10  # 每层大小倍数
    compaction_level_target_file_size_base: int = 1 * 1024 * 1024  # 基础目标文件大小(1MB)
    compaction_level0_file_num_compaction_trigger: int = 4  # Level 0文件数触发压缩的阈值
    compaction_check_interval: int = 100  # 检查是否需要压缩的写入操作次数间隔
    
    # 缓存相关
    enable_block_cache: bool = True  # 是否启用块缓存
    block_cache_size: int = 8 * 1024 * 1024  # 块缓存大小（默认8MB）
    
    # 读写相关
    max_open_files: int = 1000  # 最大打开文件数
    write_buffer_size: int = 64 * 1024  # 写缓冲区大小（默认64KB）
    
    def __post_init__(self):
        """初始化后处理，确保配置一致性。"""
        # 确保目录路径格式正确
        self.data_dir = self.data_dir.rstrip('/\\')
        self.wal_dir = self.wal_dir.rstrip('/\\')
    
    def get_data_path(self, db_path: str) -> str:
        """
        获取数据目录的完整路径。
        
        Args:
            db_path: 数据库根路径
            
        Returns:
            数据目录的完整路径
        """
        return os.path.join(db_path, self.data_dir)
    
    def get_wal_path(self, db_path: str) -> str:
        """
        获取WAL目录的完整路径。
        
        Args:
            db_path: 数据库根路径
            
        Returns:
            WAL目录的完整路径
        """
        return os.path.join(db_path, self.wal_dir)
    
    def get_bloom_filter_config(self, expected_keys: int) -> Optional[Dict[str, Any]]:
        """
        获取布隆过滤器配置。
        
        Args:
            expected_keys: 预期键数量
            
        Returns:
            布隆过滤器配置字典或None（如果禁用了布隆过滤器）
        """
        if not self.use_bloom_filter:
            return None
        
        return {
            'bits_per_key': self.bloom_filter_bits_per_key,
            'hash_count': self.bloom_filter_hash_count,
            'false_positive_rate': self.bloom_filter_false_positive_rate,
            'expected_keys': expected_keys
        }
    
    def get_level_max_size(self, level: int) -> int:
        """
        获取指定层级的最大总大小。
        
        Args:
            level: 层级编号（从0开始）
            
        Returns:
            该层级的最大总大小（字节）
        """
        if level == 0:
            # Level-0是特殊的，基于文件数量而非大小
            return self.compaction_level0_file_num_compaction_trigger * self.compaction_level_target_file_size_base
        else:
            # 其他层级的大小随层级增加而指数增长
            # Level 1的大小是Level 0的倍数，然后每层再乘以倍数
            return self.compaction_level0_file_num_compaction_trigger * self.compaction_level_target_file_size_base * (self.compaction_level_size_multiplier ** level)
    
    def get_level_target_file_size(self, level: int) -> int:
        """
        获取指定层级的目标文件大小。
        
        Args:
            level: 层级编号（从0开始）
            
        Returns:
            该层级的目标文件大小（字节）
        """
        if level <= 1:
            return self.compaction_level_target_file_size_base
        else:
            # 高层级的文件更大，每升一级大小翻倍
            return self.compaction_level_target_file_size_base * (self.compaction_level_size_multiplier ** (level - 1))
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'Config':
        """
        从字典创建配置对象。
        
        Args:
            config_dict: 包含配置选项的字典
            
        Returns:
            Config对象
        """
        config = cls()
        for key, value in config_dict.items():
            if hasattr(config, key):
                setattr(config, key, value)
            else:
                print(f"警告：未知的配置选项 '{key}'")
        return config


def default_config() -> Config:
    """
    获取默认配置。
    
    Returns:
        默认的Config对象
    """
    return Config()


def optimize_for_point_lookup(buffer_mb: int = 64) -> Config:
    """
    获取针对点查询优化的配置。
    
    Args:
        buffer_mb: 内存表大小（MB）
        
    Returns:
        优化的Config对象
    """
    config = Config()
    config.memtable_size_threshold = buffer_mb * 1024 * 1024
    config.use_bloom_filter = True
    config.bloom_filter_bits_per_key = 15  # 更多位数，降低假阳性率
    config.bloom_filter_hash_count = 10    # 更多哈希函数
    config.bloom_filter_false_positive_rate = 0.001  # 更低的假阳性率
    config.enable_block_cache = True
    config.block_cache_size = buffer_mb * 2 * 1024 * 1024  # 2倍内存表大小
    return config


def optimize_for_heavy_writes(buffer_mb: int = 128) -> Config:
    """
    获取针对大量写入优化的配置。
    
    Args:
        buffer_mb: 内存表大小（MB）
        
    Returns:
        优化的Config对象
    """
    config = Config()
    config.memtable_size_threshold = buffer_mb * 1024 * 1024  # 更大的内存表
    config.write_buffer_size = 128 * 1024  # 更大的写缓冲区
    config.compaction_level0_file_num_compaction_trigger = 8  # 更多Level 0文件
    config.compaction_trigger = 8
    config.max_file_size = 4 * 1024 * 1024  # 更大的文件
    config.use_bloom_filter = True
    # 降低压缩频率
    return config


def optimize_for_range_scan(block_kb: int = 16) -> Config:
    """
    获取针对范围扫描优化的配置。
    
    Args:
        block_kb: 数据块大小（KB）
        
    Returns:
        优化的Config对象
    """
    config = Config()
    config.sstable_block_size = block_kb * 1024  # 更大的数据块，适合范围扫描
    config.use_bloom_filter = False  # 对范围扫描帮助不大
    config.enable_block_cache = True
    config.block_cache_size = 16 * 1024 * 1024  # 更大的块缓存
    return config