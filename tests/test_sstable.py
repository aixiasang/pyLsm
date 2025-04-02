"""
测试SSTable实现。
"""
import unittest
import os
import tempfile
import shutil
import random
import string

from pylsm.sstable import SSTable
from pylsm.bloom_filter import BloomFilter


def random_string(length=10):
    """生成随机字符串"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))


class TestSSTable(unittest.TestCase):
    """测试SSTable的基本功能。"""
    
    def setUp(self):
        """测试前设置。"""
        self.test_dir = tempfile.mkdtemp()
        self.sstable_instances = []  # 跟踪创建的SSTable实例
    
    def tearDown(self):
        """测试后清理。"""
        # 首先关闭所有SSTable实例
        for sstable in self.sstable_instances:
            try:
                sstable.close()
            except:
                pass  # 忽略关闭错误
        
        # 清空实例列表
        self.sstable_instances = []
        
        # 等待一小段时间以确保文件释放
        import time
        time.sleep(0.1)
        
        # 删除测试目录
        try:
            shutil.rmtree(self.test_dir)
        except Exception as e:
            print(f"警告：无法删除测试目录 {self.test_dir}: {e}")
    
    def test_basic_write_read(self):
        """测试基本的写入和读取功能。"""
        # 准备测试数据
        data = {}
        for i in range(100):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            data[key] = value
        
        # 创建布隆过滤器并添加所有键
        bloom_filter = BloomFilter(len(data), 0.01)
        for key in data.keys():
            bloom_filter.add(key)
        
        # 写入SSTable
        sstable_path = os.path.join(self.test_dir, "test.sst")
        SSTable.write(sstable_path, data, bloom_filter=bloom_filter)
        
        # 读取SSTable
        sstable = SSTable(sstable_path)
        self.sstable_instances.append(sstable)  # 跟踪实例
        
        # 验证所有键值对是否正确读取
        for key, expected_value in data.items():
            # 首先检查布隆过滤器
            self.assertTrue(sstable.may_contain(key), f"布隆过滤器应该报告键 {key} 可能存在")
            
            # 然后获取实际值
            value = sstable.get(key)
            self.assertEqual(value, expected_value, f"键 {key} 的值应为 {expected_value}")
        
        # 测试不存在的键
        non_existent_key = b"non_existent_key"
        # 布隆过滤器可能会有假阳性，但不应该有假阴性
        # 如果布隆过滤器表示键不存在，则键肯定不存在
        if not sstable.may_contain(non_existent_key):
            self.assertIsNone(sstable.get(non_existent_key), f"不存在的键 {non_existent_key} 应返回None")
    
    def test_iterator(self):
        """测试迭代器功能。"""
        # 准备测试数据
        data = {}
        for i in range(100):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            data[key] = value
        
        # 创建布隆过滤器并添加所有键
        bloom_filter = BloomFilter(len(data), 0.01)
        for key in data.keys():
            bloom_filter.add(key)
        
        # 写入SSTable
        sstable_path = os.path.join(self.test_dir, "test.sst")
        SSTable.write(sstable_path, data, bloom_filter=bloom_filter)
        
        # 读取SSTable
        sstable = SSTable(sstable_path)
        self.sstable_instances.append(sstable)  # 跟踪实例
        
        # 使用迭代器读取所有键值对
        read_data = {key: value for key, value in sstable.items()}
        
        # 验证读取的数据是否完整和正确
        self.assertEqual(len(read_data), len(data), "读取的键值对数量应该匹配")
        for key, expected_value in data.items():
            self.assertEqual(read_data.get(key), expected_value, f"键 {key} 的值应该匹配")
        
        # 验证迭代器顺序是否按键排序
        sorted_keys = sorted(data.keys())
        iterator_keys = [key for key, _ in sstable.items()]
        self.assertEqual(iterator_keys, sorted_keys, "迭代器应按键排序返回数据")
    
    def test_get_range(self):
        """测试范围查询功能。"""
        # 准备测试数据
        data = {}
        for i in range(100):
            key = f"key{i:03d}".encode()
            value = f"value{i:03d}".encode()
            data[key] = value
        
        # 创建布隆过滤器并添加所有键
        bloom_filter = BloomFilter(len(data), 0.01)
        for key in data.keys():
            bloom_filter.add(key)
        
        # 写入SSTable
        sstable_path = os.path.join(self.test_dir, "test.sst")
        SSTable.write(sstable_path, data, bloom_filter=bloom_filter)
        
        # 读取SSTable
        sstable = SSTable(sstable_path)
        self.sstable_instances.append(sstable)  # 跟踪实例
        
        # 测试范围查询
        # 查询范围: "key020" <= key < "key030"
        start_key = b"key020"
        end_key = b"key030"
        
        expected_keys = [key for key in sorted(data.keys()) if start_key <= key < end_key]
        expected_data = {key: data[key] for key in expected_keys}
        
        # 使用范围查询
        range_data = {key: value for key, value in sstable.range(start_key, end_key)}
        
        # 验证查询结果
        self.assertEqual(len(range_data), len(expected_data), "范围查询结果数量应该匹配")
        for key, expected_value in expected_data.items():
            self.assertEqual(range_data.get(key), expected_value, f"范围查询中键 {key} 的值应该匹配")
    
    def test_bloom_filter_efficiency(self):
        """测试布隆过滤器的有效性。"""
        # 准备测试数据
        data = {}
        for i in range(1000):
            key = f"key{i:05d}".encode()
            value = f"value{i:05d}".encode()
            data[key] = value
        
        # 创建布隆过滤器并添加所有键
        bloom_filter = BloomFilter(len(data), 0.01)
        for key in data.keys():
            bloom_filter.add(key)
        
        # 写入SSTable
        sstable_path = os.path.join(self.test_dir, "test.sst")
        SSTable.write(sstable_path, data, bloom_filter=bloom_filter)
        
        # 读取SSTable
        sstable = SSTable(sstable_path)
        self.sstable_instances.append(sstable)  # 跟踪实例
        
        # 测试布隆过滤器对已存在键的检查
        for key in data.keys():
            self.assertTrue(sstable.may_contain(key), f"布隆过滤器应该报告键 {key} 可能存在")
        
        # 测试布隆过滤器对不存在键的检查
        false_positives = 0
        test_count = 1000
        for i in range(test_count):
            # 生成一个肯定不存在的键
            non_existent_key = f"nonexistent{i:05d}".encode()
            if sstable.may_contain(non_existent_key):
                false_positives += 1
        
        # 布隆过滤器的假阳性率应该不超过预期值(10%)
        false_positive_rate = false_positives / test_count
        self.assertLessEqual(false_positive_rate, 0.1, f"布隆过滤器的假阳性率应不超过10%，实际为{false_positive_rate:.2%}")


if __name__ == '__main__':
    unittest.main() 