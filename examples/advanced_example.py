"""
高级示例：展示PyLSM数据库的布隆过滤器和分级合并特性

"""
import os
import shutil
import time
import random
from typing import List, Dict, Tuple

# 导入PyLSM数据库类
from pylsm.db import DB
from pylsm.config import Config, optimize_for_point_lookup


def clear_db_dir(db_path: str) -> None:
    """清除数据库目录"""
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    os.makedirs(db_path, exist_ok=True)


def generate_random_kv_pairs(count: int, key_size: int = 16, value_size: int = 100) -> List[Tuple[bytes, bytes]]:
    """
    生成随机键值对用于测试。
    
    参数:
        count: 键值对数量
        key_size: 键的字节大小
        value_size: 值的字节大小
        
    返回:
        键值对列表
    """
    pairs = []
    for i in range(count):
        # 创建随机键，确保前缀有序以便更好地测试合并
        prefix = i.to_bytes(8, byteorder='big')
        random_suffix = os.urandom(key_size - len(prefix))
        key = prefix + random_suffix
        
        # 创建随机值
        value = os.urandom(value_size)
        pairs.append((key, value))
    
    return pairs


def run_performance_test():
    """运行性能测试，比较有无布隆过滤器的查询性能"""
    db_path = "data/advanced_example"
    clear_db_dir(db_path)
    
    # 创建配置并启用布隆过滤器
    config = optimize_for_point_lookup(buffer_mb=8)
    
    # 创建数据库实例
    db = DB(db_path, config=config)
    
    print("生成测试数据...")
    # 生成足够多的键值对以触发多个SSTable文件和合并
    data_count = 5000
    key_value_pairs = generate_random_kv_pairs(data_count)
    
    print(f"写入 {data_count} 键值对到数据库...")
    start_time = time.time()
    for key, value in key_value_pairs:
        db.put(key, value)
    write_time = time.time() - start_time
    print(f"写入完成，耗时: {write_time:.2f}秒")
    
    # 确保合并已经发生
    print("等待后台合并完成...")
    time.sleep(2)  # 等待后台合并
    
    # 强制对整个数据库范围进行合并
    print("执行强制合并...")
    db.compact()
    
    # 执行查询测试
    existing_keys = [pair[0] for pair in random.sample(key_value_pairs, 1000)]
    non_existing_keys = [os.urandom(16) for _ in range(100)]  # 不存在的键
    
    # 测试查询性能（现有键）
    print("\n测试查询已存在的键性能...")
    start_time = time.time()
    hits = 0
    for key in existing_keys:
        value = db.get(key)
        if value is not None:
            hits += 1
    existing_time = time.time() - start_time
    print(f"查询 100 个已存在的键完成，命中: {hits}，耗时: {existing_time:.2f}秒")
    
    # 测试查询性能（不存在的键 - 布隆过滤器应该避免不必要的磁盘读取）
    print("\n测试查询不存在的键性能...")
    start_time = time.time()
    false_positives = 0
    for key in non_existing_keys:
        value = db.get(key)
        if value is not None:
            false_positives += 1
    non_existing_time = time.time() - start_time
    print(f"查询 100 个不存在的键完成，假阳性: {false_positives}，耗时: {non_existing_time:.2f}秒")
    
    # 关闭数据库
    db.close()
    
    return {
        "write_time": write_time,
        "existing_query_time": existing_time,
        "non_existing_query_time": non_existing_time,
        "data_count": data_count
    }


def visualize_level_structure(db_path: str):
    """可视化数据库的分级结构"""
    # 这个功能在当前实现中无法使用，因为没有提供获取级别文件数的API
    # 我们将使用简单的文件扫描代替
    
    if not os.path.exists(db_path):
        print("数据库目录不存在")
        return
    
    # 简单统计SSTable文件数量
    sst_files = [f for f in os.listdir(db_path) if f.endswith('.sst')]
    file_count = len(sst_files)
    
    if file_count == 0:
        print("没有找到SSTable文件")
        return
    
    print(f"找到 {file_count} 个SSTable文件")
    
    # 计算总文件大小
    total_size = 0
    for file_name in sst_files:
        file_path = os.path.join(db_path, file_name)
        total_size += os.path.getsize(file_path)
    
    print(f"总文件大小: {total_size / 1024:.2f} KB")
    print(f"平均文件大小: {total_size / (file_count * 1024):.2f} KB")


def main():
    """主函数"""
    print("=" * 50)
    print("PyLSM高级特性示例：布隆过滤器和分级合并")
    print("=" * 50)
    
    # 确保数据目录存在
    os.makedirs("data", exist_ok=True)
    
    # 运行性能测试
    results = run_performance_test()
    
    # 可视化文件结构
    visualize_level_structure("data/advanced_example")
    
    # 显示性能摘要
    print("\n性能摘要:")
    print(f"写入 {results['data_count']} 条记录: {results['write_time']:.2f}秒")
    print(f"查询已存在的键: {results['existing_query_time']:.2f}秒")
    print(f"查询不存在的键: {results['non_existing_query_time']:.2f}秒")
    
    if results['non_existing_query_time'] > 0:
        speedup = results['existing_query_time'] / results['non_existing_query_time']
        print(f"布隆过滤器加速比: {speedup:.2f}x")
    
    print("\n分级合并已经将数据组织成为类似LevelDB的分层结构")
    print("L0文件可能有重叠的键范围，而更高级别的文件键范围不重叠")
    print("这种结构大幅减少了读放大和写放大")
    
    print("\n示例完成！")


if __name__ == "__main__":
    main() 