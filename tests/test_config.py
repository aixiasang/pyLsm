"""
测试配置模块。
"""
import unittest
import os

from pylsm.config import Config


class TestConfig(unittest.TestCase):
    """测试配置模块的功能。"""
    
    def test_default_values(self):
        """测试默认配置值是否正确。"""
        config = Config()
        
        # 测试基本目录配置
        self.assertEqual(config.data_dir, 'data')
        self.assertEqual(config.wal_dir, 'wal')
        
        # 测试MemTable配置
        self.assertEqual(config.memtable_size_threshold, 4 * 1024 * 1024)  # 4MB
        self.assertEqual(config.skiplist_max_level, 12)
        self.assertEqual(config.skiplist_p, 0.5)
        
        # 测试SSTable配置
        self.assertEqual(config.sstable_block_size, 4 * 1024)  # 4KB
        self.assertTrue(config.use_bloom_filter)
        
        # 测试压缩配置
        self.assertEqual(config.compaction_trigger, 4)
        self.assertEqual(config.compaction_max_level, 7)
    
    def test_custom_values(self):
        """测试自定义配置值是否正确设置。"""
        custom_config = {
            'data_dir': 'custom_data',
            'memtable_size_threshold': 8 * 1024 * 1024,  # 8MB
            'use_bloom_filter': False,
            'compaction_max_level': 10
        }
        
        config = Config(**custom_config)
        
        # 测试自定义值
        self.assertEqual(config.data_dir, 'custom_data')
        self.assertEqual(config.memtable_size_threshold, 8 * 1024 * 1024)
        self.assertFalse(config.use_bloom_filter)
        self.assertEqual(config.compaction_max_level, 10)
        
        # 测试未自定义的值应该保持默认
        self.assertEqual(config.wal_dir, 'wal')
        self.assertEqual(config.skiplist_max_level, 12)
    
    def test_path_helpers(self):
        """测试路径辅助方法是否正确。"""
        config = Config(data_dir='custom_data', wal_dir='custom_wal')
        db_path = '/tmp/testdb'
        
        # 测试数据路径
        expected_data_path = os.path.join(db_path, 'custom_data')
        self.assertEqual(config.get_data_path(db_path), expected_data_path)
        
        # 测试WAL路径
        expected_wal_path = os.path.join(db_path, 'custom_wal')
        self.assertEqual(config.get_wal_path(db_path), expected_wal_path)
    
    def test_bloom_filter_config(self):
        """测试布隆过滤器配置生成是否正确。"""
        # 测试默认配置
        config = Config()
        bloom_config = config.get_bloom_filter_config(1000)
        
        self.assertEqual(bloom_config['bits_per_key'], 10)
        self.assertEqual(bloom_config['hash_count'], 7)
        self.assertEqual(bloom_config['false_positive_rate'], 0.01)
        self.assertEqual(bloom_config['expected_keys'], 1000)
        
        # 测试禁用布隆过滤器
        config = Config(use_bloom_filter=False)
        bloom_config = config.get_bloom_filter_config(1000)
        self.assertIsNone(bloom_config)
        
        # 测试自定义布隆过滤器参数
        config = Config(
            bloom_filter_bits_per_key=20,
            bloom_filter_hash_count=5,
            bloom_filter_false_positive_rate=0.001
        )
        bloom_config = config.get_bloom_filter_config(2000)
        
        self.assertEqual(bloom_config['bits_per_key'], 20)
        self.assertEqual(bloom_config['hash_count'], 5)
        self.assertEqual(bloom_config['false_positive_rate'], 0.001)
        self.assertEqual(bloom_config['expected_keys'], 2000)
    
    def test_level_size_calculation(self):
        """测试层级大小计算是否正确。"""
        config = Config(
            compaction_level_target_file_size_base=1 * 1024 * 1024,  # 1MB
            compaction_level_size_multiplier=10,
            compaction_level0_file_num_compaction_trigger=4
        )
        
        # 测试Level 0的目标文件大小
        self.assertEqual(config.get_level_target_file_size(0), 1 * 1024 * 1024)
        
        # 测试Level 1的目标文件大小
        self.assertEqual(config.get_level_target_file_size(1), 1 * 1024 * 1024)
        
        # 测试Level 2的目标文件大小 (应该是Level 1的10倍)
        self.assertEqual(config.get_level_target_file_size(2), 10 * 1024 * 1024)
        
        # 测试Level 3的目标文件大小 (应该是Level 1的100倍)
        self.assertEqual(config.get_level_target_file_size(3), 100 * 1024 * 1024)
        
        # 测试Level 0的最大大小 (4个文件 * 1MB)
        self.assertEqual(config.get_level_max_size(0), 4 * 1024 * 1024)
        
        # 测试Level 1的最大大小 (Level 0的10倍)
        self.assertEqual(config.get_level_max_size(1), 40 * 1024 * 1024)
        
        # 测试Level 2的最大大小 (Level 1的10倍)
        self.assertEqual(config.get_level_max_size(2), 400 * 1024 * 1024)


if __name__ == '__main__':
    unittest.main() 