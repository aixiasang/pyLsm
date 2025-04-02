"""
PyLSM数据库基本用法示例

该示例演示了PyLSM数据库的基本API使用，包括:
1. 基本的增删改查操作
2. 配置布隆过滤器
3. 查看和触发分层压缩
"""
import os
import time
import shutil
import random
import string
from pylsm.db import DB
from pylsm.config import Config, optimize_for_point_lookup


def random_string(length: int = 10) -> str:
    """生成随机字符串。"""
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))


def clean_directory(path: str, max_retries=3, retry_delay=0.5):
    """安全清理目录，处理文件句柄释放问题"""
    if not os.path.exists(path):
        return
    
    for attempt in range(max_retries):
        try:
            shutil.rmtree(path)
            print(f"已删除目录: {path}")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"删除目录失败，等待重试: {e}")
                time.sleep(retry_delay * (attempt + 1))
            else:
                print(f"无法删除目录: {e}")


def main():
    # 创建数据库目录
    db_path = "testdb"
    clean_directory(db_path)
    os.makedirs(db_path, exist_ok=True)
    
    # 使用上下文管理器确保资源释放
    try:
        print("=== 创建优化的数据库配置 ===")
        # 创建针对点查询优化的配置，启用布隆过滤器
        config = optimize_for_point_lookup(buffer_mb=2)  # 2MB内存表，为了演示更快触发压缩
        print(f"内存表大小: {config.memtable_size_threshold / 1024 / 1024} MB")
        print(f"启用布隆过滤器: {config.use_bloom_filter}")
        print(f"布隆过滤器位/键: {config.bloom_filter_bits_per_key}")
        print(f"布隆过滤器假阳性率: {config.bloom_filter_false_positive_rate}")
        
        print("\n=== 打开数据库 ===")
        db = DB(db_path, config)
        
        # 写入一批数据，足够填满一个内存表并触发刷写
        print("\n=== 写入数据 ===")
        num_entries = 5000
        keys = []
        for i in range(num_entries):
            key = f"key_{i}_{random_string(10)}"
            value = f"value_{i}_{random_string(50)}"
            db.put(key, value)
            if i % 1000 == 0:
                print(f"已写入 {i} 条记录")
            keys.append(key)
        
        print(f"总共写入 {num_entries} 条记录")
        
        # 进行一些查询操作
        print("\n=== 查询操作 ===")
        # 随机选择10个键进行查询
        for _ in range(10):
            key = random.choice(keys)
            value = db.get(key)
            print(f"查询 {key}: {'找到' if value else '未找到'}")
        
        # 查询不存在的键
        for _ in range(5):
            key = f"nonexistent_{random_string(20)}"
            start_time = time.time()
            value = db.get(key)
            end_time = time.time()
            print(f"查询不存在的键 {key}: 用时 {(end_time - start_time) * 1000:.2f} ms")
        
        # 删除一些键
        print("\n=== 删除操作 ===")
        for i in range(10):
            key = keys[i]
            db.delete(key)
            print(f"删除 {key}")
            
            # 验证删除
            value = db.get(key)
            print(f"验证删除 {key}: {'未删除' if value else '已删除'}")
        
        # 再次写入更多数据触发压缩
        print("\n=== 写入更多数据触发压缩 ===")
        more_keys = []
        for i in range(num_entries, num_entries * 2):
            key = f"key_{i}_{random_string(10)}"
            value = f"value_{i}_{random_string(50)}"
            db.put(key, value)
            if i % 1000 == 0:
                print(f"已写入 {i} 条记录")
            more_keys.append(key)
        
        # 添加所有键到keys列表
        keys.extend(more_keys)
        
        # 手动触发压缩
        print("\n=== 手动触发压缩 ===")
        db.compact()
        print("压缩完成")
        
        # 测试范围查询
        print("\n=== 范围查询 ===")
        start_key = "key_1000"
        end_key = "key_1100"
        
        print(f"范围查询: {start_key} 到 {end_key}")
        results = []
        for key, value in db.range(start_key, end_key):
            results.append((key, value))
        
        print(f"获取到 {len(results)} 条结果")
        
        # 显示部分结果
        for key, value in results[:5]:
            print(f"范围查询结果: {key} -> {value.decode('utf-8')[:20]}...")
        
        # 关闭数据库并测试持久性
        print("\n=== 测试持久性 ===")
        db.flush()
        db.close()
        
        # 等待一段时间确保文件句柄释放
        time.sleep(1)
        
        # 重新打开数据库并进行一些查询
        print("\n=== 重新打开数据库 ===")
        
        db = None  # 确保前一个DB对象被垃圾回收
        time.sleep(0.5)  # 给系统一些时间来释放资源
        
        try:
            db2 = DB(db_path, config)
            
            print("\n=== 验证数据持久化 ===")
            hits = 0
            num_queries = 20
            test_keys = random.sample(keys[10:], num_queries)  # 跳过已删除的键
            
            for key in test_keys:
                value = db2.get(key)
                hit = value is not None
                if hit:
                    hits += 1
                print(f"查询 {key}: {'找到' if hit else '未找到'}")
            
            print(f"总共查询 {num_queries} 个键，找到 {hits} 个（命中率: {hits/num_queries*100:.1f}%）")
            
            # 关闭数据库
            db2.close()
            print("数据库已关闭")
        except Exception as e:
            print(f"重新打开数据库时出错: {e}")
        
        print("\n=== 示例完成 ===")
    
    finally:
        # 确保DB对象被释放
        db = None
        db2 = None
        
        # 等待文件句柄释放
        time.sleep(1)
        
        # 清理测试目录
        clean_directory(db_path)


if __name__ == "__main__":
    main() 