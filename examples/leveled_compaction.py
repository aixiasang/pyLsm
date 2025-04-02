"""
PyLSM分层压缩与布隆过滤器示例。

本示例展示了PyLSM的分层压缩和布隆过滤器功能，并对比了使用这些优化前后的性能差异。
主要测试内容包括：
1. 写入性能：通过批量插入测试写入速度
2. 点查询性能：使用/不使用布隆过滤器时的随机查询性能
3. 范围查询性能：在不同压缩策略下的范围查询性能
4. 空间利用率：分析在分层压缩下的存储空间使用情况
"""
import os
import sys
import time
import random
import shutil
import string
from typing import List, Dict, Tuple

# 添加项目根目录到模块搜索路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pylsm.db import DB
from pylsm.config import Config, optimize_for_point_lookup, optimize_for_heavy_writes


def generate_random_kv_pairs(count: int, key_size: int = 16, value_size: int = 100) -> List[Tuple[bytes, bytes]]:
    """生成随机键值对。"""
    pairs = []
    for _ in range(count):
        key = ''.join(random.choice(string.ascii_letters) for _ in range(key_size)).encode()
        value = ''.join(random.choice(string.ascii_letters) for _ in range(value_size)).encode()
        pairs.append((key, value))
    return pairs


def benchmark_write(db_path: str, kv_pairs: List[Tuple[bytes, bytes]], config: Config = None) -> float:
    """测试写入性能。"""
    # 清理旧数据库目录
    try:
        if os.path.exists(db_path):
            for attempt in range(3):
                try:
                    shutil.rmtree(db_path)
                    break
                except (PermissionError, OSError) as e:
                    print(f"删除目录失败，等待重试: {e}")
                    time.sleep(1)
            else:
                print(f"警告: 无法删除目录 {db_path}，尝试继续...")
    except Exception as e:
        print(f"清理旧数据库目录时出错: {e}")
    
    # 创建数据库
    db = DB(db_path, config)
    
    # 写入测试
    start_time = time.time()
    for key, value in kv_pairs:
        db.put(key, value)
    elapsed_time = time.time() - start_time
    
    # 关闭数据库
    db.close()
    
    return elapsed_time


def benchmark_read(db_path: str, keys: List[bytes], config: Config = None) -> Tuple[float, int]:
    """测试读取性能。"""
    # 打开数据库
    db = DB(db_path, config=config)
    
    # 读取测试
    start_time = time.time()
    hit_count = 0
    for key in keys:
        value = db.get(key)
        if value is not None:
            hit_count += 1
    elapsed = time.time() - start_time
    
    # 关闭数据库
    db.close()
    
    return elapsed, hit_count


def benchmark_range_query(db_path: str, start_keys: List[bytes], config: Config = None) -> float:
    """测试范围查询性能。"""
    # 打开数据库
    db = DB(db_path, config=config)
    
    # 范围查询测试
    start_time = time.time()
    total_items = 0
    for start_key in start_keys:
        # 为每个起始键设置一个结束键
        end_key = start_key + b'\xff'  # 使用一个比当前键大的键作为结束键
        
        # 统计范围内的项数
        count = 0
        for _, _ in db.range(start_key, end_key):
            count += 1
            if count >= 100:  # 限制每次查询最多100个项
                break
        total_items += count
    
    elapsed = time.time() - start_time
    
    # 关闭数据库
    db.close()
    
    return elapsed


def analyze_storage_efficiency(db_path: str) -> Dict:
    """分析存储效率。"""
    # 由于当前实现没有提供获取统计信息的API，我们简单地统计SSTable文件
    result = {}
    
    # 检查SSTable文件和大小
    if os.path.exists(db_path):
        files = [f for f in os.listdir(db_path) if f.endswith('.sst')]
        total_size = 0
        for file in files:
            file_path = os.path.join(db_path, file)
            total_size += os.path.getsize(file_path)
        
        result = {
            'file_count': len(files),
            'total_size': total_size,
            'avg_file_size': total_size / len(files) if files else 0
        }
    
    return result


def print_results(title: str, results: Dict):
    """打印测试结果。"""
    print(f"\n{title}")
    print("=" * 50)
    for test, value in results.items():
        if isinstance(value, float):
            print(f"{test}: {value:.4f} seconds")
        else:
            print(f"{test}: {value}")
    print("=" * 50)


def main():
    """主函数，运行所有测试。"""
    # 测试参数
    DB_PATH_DEFAULT = "test_db_default"
    DB_PATH_OPTIMIZED = "test_db_optimized"
    NUM_PAIRS = 10000
    NUM_QUERIES = 1000
    
    # 生成测试数据
    print(f"生成{NUM_PAIRS}个键值对...")
    kv_pairs = generate_random_kv_pairs(NUM_PAIRS)
    
    # 提取所有键
    all_keys = [key for key, _ in kv_pairs]
    
    # 创建用于查询的随机键集
    random.shuffle(all_keys)
    query_keys = all_keys[:NUM_QUERIES]
    
    # 创建用于范围查询的起始键
    range_start_keys = random.sample(all_keys, 10)
    
    # 默认配置测试
    default_config = Config()
    
    # 优化配置测试（启用分层压缩和布隆过滤器）
    optimized_config = optimize_for_point_lookup(buffer_mb=1)  # 1MB内存表，更频繁刷写
    
    # 执行写入测试
    print("\n开始写入测试...")
    write_time_default = benchmark_write(DB_PATH_DEFAULT, kv_pairs, default_config)
    write_time_optimized = benchmark_write(DB_PATH_OPTIMIZED, kv_pairs, optimized_config)
    
    # 执行点查询测试
    print("\n开始点查询测试...")
    read_time_default, hit_count_default = benchmark_read(DB_PATH_DEFAULT, query_keys, default_config)
    read_time_optimized, hit_count_optimized = benchmark_read(DB_PATH_OPTIMIZED, query_keys, optimized_config)
    
    # 执行范围查询测试
    print("\n开始范围查询测试...")
    range_time_default = benchmark_range_query(DB_PATH_DEFAULT, range_start_keys, default_config)
    range_time_optimized = benchmark_range_query(DB_PATH_OPTIMIZED, range_start_keys, optimized_config)
    
    # 执行存储效率分析
    print("\n分析存储效率...")
    storage_default = analyze_storage_efficiency(DB_PATH_DEFAULT)
    storage_optimized = analyze_storage_efficiency(DB_PATH_OPTIMIZED)
    
    # 手动触发压缩（优化配置）
    print("\n手动触发压缩...")
    db = DB(DB_PATH_OPTIMIZED, config=optimized_config)
    db.compact()
    db.close()
    
    # 再次分析存储效率（优化配置，压缩后）
    storage_after_compaction = analyze_storage_efficiency(DB_PATH_OPTIMIZED)
    
    # 打印结果
    write_results = {
        "默认配置": write_time_default,
        "优化配置": write_time_optimized,
        "性能提升": f"{(write_time_default / write_time_optimized - 1) * 100:.2f}%" if write_time_optimized > 0 else "N/A"
    }
    print_results("写入性能", write_results)
    
    read_results = {
        "默认配置": read_time_default,
        "优化配置": read_time_optimized,
        "命中数量（默认）": hit_count_default,
        "命中数量（优化）": hit_count_optimized,
        "性能提升": f"{(read_time_default / read_time_optimized - 1) * 100:.2f}%" if read_time_optimized > 0 else "N/A"
    }
    print_results("点查询性能", read_results)
    
    range_results = {
        "默认配置": range_time_default,
        "优化配置": range_time_optimized,
        "性能提升": f"{(range_time_default / range_time_optimized - 1) * 100:.2f}%" if range_time_optimized > 0 else "N/A"
    }
    print_results("范围查询性能", range_results)
    
    print("\n存储效率分析（默认配置）")
    print(f"文件数量: {storage_default.get('file_count', 0)}")
    print(f"总大小: {storage_default.get('total_size', 0) / 1024:.2f} KB")
    print(f"平均文件大小: {storage_default.get('avg_file_size', 0) / 1024:.2f} KB")
    
    print("\n存储效率分析（优化配置，压缩前）")
    print(f"文件数量: {storage_optimized.get('file_count', 0)}")
    print(f"总大小: {storage_optimized.get('total_size', 0) / 1024:.2f} KB")
    print(f"平均文件大小: {storage_optimized.get('avg_file_size', 0) / 1024:.2f} KB")
    
    print("\n存储效率分析（优化配置，压缩后）")
    print(f"文件数量: {storage_after_compaction.get('file_count', 0)}")
    print(f"总大小: {storage_after_compaction.get('total_size', 0) / 1024:.2f} KB")
    print(f"平均文件大小: {storage_after_compaction.get('avg_file_size', 0) / 1024:.2f} KB")
    
    # 清理测试数据库
    print("\n清理测试文件...")
    shutil.rmtree(DB_PATH_DEFAULT, ignore_errors=True)
    shutil.rmtree(DB_PATH_OPTIMIZED, ignore_errors=True)
    
    print("\n测试完成！")


if __name__ == "__main__":
    main() 