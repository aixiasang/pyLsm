"""
布隆过滤器测试模块。
"""

import pytest
import os
import random
import string
from pylsm.bloom_filter import BloomFilter, create_optimal_bloom_filter


def random_key(length=10):
    """生成随机键。"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(length)).encode('utf-8')


class TestBloomFilter:
    
    def test_initialization(self):
        """测试布隆过滤器初始化。"""
        # 使用直接参数方式初始化
        bf = BloomFilter(10, 7.0)  # 第二个参数作为浮点数传递，但会被转为整数
        assert bf.bits_per_key == 10
        assert bf.num_hashes == 7
        assert bf.num_bits == 0  # 初始时位数组为空
        assert bf.num_keys == 0
        
        # 测试容量和错误率方式初始化
        bf2 = BloomFilter(100, 0.01)
        assert bf2.bits_per_key > 0
        assert bf2.num_hashes > 0
        assert bf2.expected_elements == 100
        assert bf2.false_positive_rate == 0.01
    
    def test_create_for_capacity(self):
        """测试为指定容量创建布隆过滤器。"""
        capacity = 1000
        fpr = 0.01
        bf = BloomFilter.create_for_capacity(capacity, fpr)
        
        # 检查参数是否合理
        assert bf.bits_per_key > 0
        assert bf.num_hashes > 0
        assert bf.bit_array_size >= capacity * bf.bits_per_key
        
        # 验证位数组被正确初始化
        assert len(bf.bit_array) == (bf.bit_array_size + 7) // 8
    
    def test_add_and_contains(self):
        """测试添加元素和检查元素存在。"""
        bf = BloomFilter.create_for_capacity(100, 0.01)
        
        # 添加一些键
        keys = [random_key() for _ in range(50)]
        for key in keys:
            bf.add(key)
        
        # 所有添加的键都应该存在
        for key in keys:
            assert bf.may_contain(key) == True
        
        # 验证假阳性率在合理范围内
        false_positives = 0
        trials = 1000
        for _ in range(trials):
            key = random_key()
            # 确保这是一个新键
            while key in keys:
                key = random_key()
            
            if bf.may_contain(key):
                false_positives += 1
        
        false_positive_rate = false_positives / trials
        # 假阳性率不应太高，但也无法保证精确的值
        assert false_positive_rate < 0.05
    
    def test_serialization(self):
        """测试布隆过滤器的序列化和反序列化。"""
        # 创建和填充布隆过滤器
        original = BloomFilter.create_for_capacity(100, 0.01)
        keys = [random_key() for _ in range(50)]
        for key in keys:
            original.add(key)
        
        # 序列化
        serialized = original.to_bytes()
        
        # 反序列化
        deserialized = BloomFilter.from_bytes(serialized)
        
        # 检查属性是否一致
        assert deserialized.bits_per_key == original.bits_per_key
        assert deserialized.num_hashes == original.num_hashes
        assert deserialized.num_bits == original.num_bits
        assert deserialized.num_keys == original.num_keys
        
        # 检查所有键都可以被检测到
        for key in keys:
            assert deserialized.may_contain(key) == True
    
    def test_create_optimal_helper(self):
        """测试创建最优布隆过滤器的辅助函数。"""
        bf = create_optimal_bloom_filter(1000, 0.001)
        assert bf.bits_per_key > 0
        assert bf.num_hashes > 0
        
        # 检查参数是否合理 - 更低的假阳性率需要更多的位/键
        bf_high_fpr = create_optimal_bloom_filter(1000, 0.1)
        bf_low_fpr = create_optimal_bloom_filter(1000, 0.001)
        assert bf_low_fpr.bits_per_key > bf_high_fpr.bits_per_key
    
    def test_resize(self):
        """测试位数组的动态调整大小。"""
        # 使用直接参数方式初始化
        bf = BloomFilter(10, 7.0)  # 使用较小的初始容量触发重新调整大小
        # 添加足够多的键触发扩展
        initial_size = 0
        for i in range(100):
            key = f"key_{i}".encode('utf-8')
            if i == 10:  # 记录添加一些键后的大小
                initial_size = bf.num_bits
            bf.add(key)
        
        # 验证位数组确实扩大了
        assert bf.num_bits > initial_size
    
    def test_fill_ratio(self):
        """测试填充率计算。"""
        bf = BloomFilter.create_for_capacity(100, 0.01)
        # 初始填充率应该为0或很小
        initial_ratio = bf._get_fill_ratio()
        
        # 添加键后填充率应该增加
        for i in range(50):
            bf.add(f"key_{i}".encode('utf-8'))
        
        new_ratio = bf._get_fill_ratio()
        assert new_ratio > initial_ratio 