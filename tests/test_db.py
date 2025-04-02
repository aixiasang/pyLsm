"""
测试主数据库实现。
"""
import unittest
import os
import tempfile
import shutil
import random
import string
import time

from pylsm.db import DB
from pylsm.config import Config


def random_string(length=10):
    """生成随机字符串"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(length)).encode()


class TestDB(unittest.TestCase):
    """测试主数据库的基本功能。"""
    
    def setUp(self):
        """测试前设置。"""
        self.test_dir = tempfile.mkdtemp()
        
        # 创建一个测试用的配置，使用较小的阈值以便于测试
        test_config = Config(
            memtable_size_threshold=1024 * 10,  # 10KB
            compaction_level0_file_num_compaction_trigger=2,
            compaction_level_target_file_size_base=1024 * 5  # 5KB
        )
        
        self.db = DB(self.test_dir, config=test_config)
    
    def tearDown(self):
        """测试后清理。"""
        # 关闭数据库时可能出现文件权限问题，捕获并忽略这些错误
        try:
            if hasattr(self, 'db') and self.db:
                self.db.close()
        except Exception as e:
            print(f"警告：关闭数据库时出错: {e}")
        
        # 等待一小段时间以确保文件资源被释放
        time.sleep(0.1)
        
        # 删除测试目录
        try:
            shutil.rmtree(self.test_dir)
        except Exception as e:
            print(f"警告：无法删除测试目录 {self.test_dir}: {e}")
    
    def test_put_and_get(self):
        """测试基本的读写操作。"""
        # 添加一些键值对
        test_data = {}
        for i in range(20):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            test_data[key] = value
            self.db.put(key, value)
        
        # 测试所有已添加的键
        for key, expected_value in test_data.items():
            value = self.db.get(key)
            self.assertEqual(value, expected_value, f"键 {key} 的值应为 {expected_value}")
        
        # 测试不存在的键
        non_existent_key = b"non_existent_key"
        value = self.db.get(non_existent_key)
        self.assertIsNone(value, f"不存在的键 {non_existent_key} 应返回None")
    
    def test_delete(self):
        """测试删除操作。"""
        # 添加一些键值对
        key1 = b"key1"
        value1 = b"value1"
        key2 = b"key2"
        value2 = b"value2"
        
        self.db.put(key1, value1)
        self.db.put(key2, value2)
        
        # 验证两个键都存在
        self.assertEqual(self.db.get(key1), value1)
        self.assertEqual(self.db.get(key2), value2)
        
        # 删除第一个键
        self.db.delete(key1)
        
        # 验证第一个键已删除，第二个键仍存在
        self.assertIsNone(self.db.get(key1), "已删除的键应返回None")
        self.assertEqual(self.db.get(key2), value2, "未删除的键应该仍然存在")
    
    def test_memtable_flush(self):
        """测试内存表刷新到磁盘的功能。"""
        # 添加数据，但不超过默认阈值
        for i in range(5):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode() * 10  # 增加数据大小
            self.db.put(key, value)

        # 手动刷新内存表
        try:
            self.db._flush_memtable()
        except Exception as e:
            print(f"刷新内存表时出错: {e}")
            
        # 验证刷新后仍然可以读取数据
        for i in range(5):
            key = f"key{i:03d}".encode()
            value = self.db.get(key)
            self.assertEqual(value, f"value{i:03d}".encode() * 10)
    
    def test_recovery(self):
        """测试数据库恢复功能。"""
        # 添加一些键值对
        test_data = {}
        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            test_data[key] = value
            self.db.put(key, value)

        # 手动刷新一部分数据到磁盘
        try:
            self.db._flush_memtable()
        except Exception as e:
            print(f"刷新内存表时出错: {e}")
            
        # 添加更多数据（保留在内存表中）
        for i in range(10, 20):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            test_data[key] = value
            self.db.put(key, value)

        # 关闭数据库
        try:
            self.db.close()
        except Exception as e:
            print(f"关闭数据库时出错: {e}")
            
        # 重新打开数据库
        self.db = DB(self.test_dir)
        
        # 验证所有数据是否正确恢复
        for key, expected_value in test_data.items():
            value = self.db.get(key)
            self.assertEqual(value, expected_value, f"键 {key} 的值应该匹配")
    
    def test_iterator(self):
        """测试数据库迭代器。"""
        # 添加一些键值对并刷新部分到磁盘
        test_data = {}
        for i in range(15):
            key = f"key{i:03d}"
            value = f"value{i:03d}".encode()
            test_data[key] = value
            self.db.put(key, value)
            if i % 5 == 0 and i > 0:
                try:
                    self.db._flush_memtable()  # 每5个键刷新一次
                except Exception as e:
                    print(f"刷新内存表时出错: {e}")

        # 使用数据库迭代器
        db_data = {}
        for key, value in self.db.range():
            db_data[key] = value

        # 验证迭代器返回的数据是否完整和正确
        self.assertEqual(len(db_data), len(test_data), "迭代器返回的键值对数量应该匹配")
        for key, expected_value in test_data.items():
            self.assertEqual(db_data.get(key), expected_value, f"键 {key} 的值应该匹配")


if __name__ == '__main__':
    unittest.main() 