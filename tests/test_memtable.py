"""
测试MemTable实现。
"""
import unittest
import os
import tempfile
import random
import string

from pylsm.memtable import MemTable
from pylsm.wal import WAL


def random_string(length=10):
    """生成随机字符串"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(length)).encode()


class TestMemTable(unittest.TestCase):
    """测试MemTable的基本功能。"""
    
    def setUp(self):
        """测试前设置。"""
        self.temp_dir = tempfile.mkdtemp()
        self.wal_path = os.path.join(self.temp_dir, "test.wal")
        self.wal = WAL(self.wal_path)
        self.memtable = MemTable(self.wal)
    
    def tearDown(self):
        """测试后清理。"""
        self.wal.close()
        # 删除wal文件
        if os.path.exists(self.wal_path):
            os.remove(self.wal_path)
        # 删除临时目录
        os.rmdir(self.temp_dir)
    
    def test_put_and_get(self):
        """测试放入和获取数据。"""
        # 添加一些键值对
        test_data = {}
        for i in range(100):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            test_data[key] = value
            self.memtable.put(key, value)
        
        # 测试所有已添加的键
        for key, expected_value in test_data.items():
            value = self.memtable.get(key)
            self.assertEqual(value, expected_value, f"键 {key} 的值应为 {expected_value}")
        
        # 测试不存在的键
        non_existent_key = b"non_existent_key"
        value = self.memtable.get(non_existent_key)
        self.assertIsNone(value, f"不存在的键 {non_existent_key} 应返回None")
    
    def test_delete(self):
        """测试删除操作。"""
        # 添加一些键值对
        key1 = b"key1"
        value1 = b"value1"
        key2 = b"key2"
        value2 = b"value2"
        
        self.memtable.put(key1, value1)
        self.memtable.put(key2, value2)
        
        # 验证两个键都存在
        self.assertEqual(self.memtable.get(key1), value1)
        self.assertEqual(self.memtable.get(key2), value2)
        
        # 删除第一个键
        self.memtable.delete(key1)
        
        # 验证第一个键已删除，第二个键仍存在
        self.assertIsNone(self.memtable.get(key1), "已删除的键应返回None")
        self.assertEqual(self.memtable.get(key2), value2, "未删除的键应该仍然存在")
    
    def test_size_and_empty(self):
        """测试大小和空检查。"""
        # 初始应该为空
        self.assertEqual(self.memtable.size(), 0, "新创建的MemTable应该为空")
        self.assertTrue(self.memtable.is_empty(), "新创建的MemTable应该为空")
        
        # 添加一些键值对
        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            self.memtable.put(key, value)
            # 验证大小正确增加
            self.assertEqual(self.memtable.size(), i + 1, f"添加 {i+1} 个键值对后大小应为 {i+1}")
            self.assertFalse(self.memtable.is_empty(), f"添加 {i+1} 个键值对后MemTable不应该为空")
        
        # 删除一个键
        self.memtable.delete(b"key001")
        # 因为删除操作会添加一个删除标记，所以大小不会减少
        self.assertEqual(self.memtable.size(), 10, "删除一个键后大小应保持不变，因为会添加删除标记")
        
        # 清空MemTable
        new_wal = WAL(os.path.join(self.temp_dir, "test2.wal"))
        new_memtable = MemTable(new_wal)
        self.assertEqual(new_memtable.size(), 0, "新创建的MemTable应该为空")
        self.assertTrue(new_memtable.is_empty(), "新创建的MemTable应该为空")
        
        # 清理
        new_wal.close()
        os.remove(os.path.join(self.temp_dir, "test2.wal"))
    
    def test_iterator(self):
        """测试迭代器。"""
        # 添加一些键值对
        test_data = {}
        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            test_data[key] = value
            self.memtable.put(key, value)
        
        # 使用迭代器读取所有键值对
        read_data = {key: value for key, value in self.memtable.items()}
        
        # 验证读取的数据是否完整且有序
        self.assertEqual(len(read_data), len(test_data), "读取的键值对数量应该匹配")
        
        # 验证顺序是按键排序的
        sorted_items = sorted(read_data.items())
        iterator_items = list(self.memtable.items())
        
        for i, (key, value) in enumerate(iterator_items):
            expected_key, expected_value = sorted_items[i]
            self.assertEqual(key, expected_key, f"第 {i} 个键应该是 {expected_key}")
            self.assertEqual(value, expected_value, f"第 {i} 个值应该是 {expected_value}")
    
    def test_recovery_from_wal(self):
        """测试从WAL恢复。"""
        # 添加一些键值对
        test_data = {}
        for i in range(10):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            test_data[key] = value
            self.memtable.put(key, value)
        
        # 关闭当前memtable
        self.wal.close()
        
        # 创建新的WAL和MemTable，应该从WAL文件恢复
        new_wal = WAL(self.wal_path)
        new_memtable = MemTable(new_wal)
        
        # 验证数据已恢复
        for key, expected_value in test_data.items():
            value = new_memtable.get(key)
            self.assertEqual(value, expected_value, f"从WAL恢复后，键 {key} 的值应为 {expected_value}")
        
        # 清理
        new_wal.close()


if __name__ == '__main__':
    unittest.main() 