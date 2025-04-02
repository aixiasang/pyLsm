# PyLSM: Python LSM树键值存储引擎

PyLSM是一个基于LSM树(Log-Structured Merge Tree)架构的高性能键值存储引擎，使用Python实现。本项目不仅提供了高效的键值对存储和检索功能，还包含了布隆过滤器和分层压缩等高级特性。

## 🌟 核心特性

- 📦 LSM树架构：实现了完整的LSM树数据结构
- 🚀 高性能读写：优化的读写路径设计
- 🔍 范围查询：支持高效的范围扫描操作
- 🌸 布隆过滤器：减少不必要的磁盘访问
- 📊 分层压缩：智能的文件合并策略
- 🛠️ 命令行工具：交互式数据操作界面
- 💾 数据持久化：支持崩溃恢复
- 🔄 自动压缩：后台自动进行文件合并

## 📚 教学价值

本项目特别适合以下学习场景：

1. **数据库原理课程**：
   - LSM树数据结构的实际实现
   - 存储引擎的核心概念
   - 数据持久化机制

2. **系统设计课程**：
   - 高性能系统架构设计
   - 并发控制实现
   - 文件系统交互

3. **Python高级编程**：
   - 面向对象设计模式
   - 文件IO处理
   - 性能优化技术

## 🔧 安装说明

```bash
# 克隆项目
git clone https://github.com/aixiasang/pyLsm.git
cd pyLsm

# 安装依赖
pip install -r requirements.txt
```

## 🚀 快速入门

### 基础操作示例

```python
from pylsm.db import DB

# 创建数据库实例
db = DB("./test_db")

# 写入键值对
db.put(b"hello", b"world")
db.put(b"name", b"PyLSM")

# 读取值
value = db.get(b"hello")  # 返回 b"world"
print(f"读取结果: {value.decode()}")

# 范围查询
print("遍历所有键值对:")
for key, value in db.range(b"a", b"z"):
    print(f"键: {key.decode()}, 值: {value.decode()}")

# 删除键
db.delete(b"hello")

# 关闭数据库
db.close()
```

### 命令行工具使用

PyLSM提供了强大的命令行工具，方便进行交互式操作：

```bash
python -m pylsm.cli ./my_db
```

支持的命令：
```
open [--no-create]  # 打开数据库
close              # 关闭数据库
put <key> <value>  # 插入键值对
get <key>          # 获取值
delete <key>       # 删除键
scan               # 范围扫描
  --start <key>    # 起始键
  --end <key>      # 结束键
  --limit <n>      # 限制返回数量
compact            # 手动触发压缩
info               # 显示数据库信息
help               # 显示帮助信息
exit               # 退出CLI
```

## 📖 深入理解LSM树

### LSM树工作原理

1. **写入流程**：
   ```
   内存表(MemTable)
        ↓
   不可变内存表(Immutable MemTable)
        ↓
   SSTable文件(Level 0)
        ↓
   分层压缩(Level 1-N)
   ```

2. **读取流程**：
   ```
   查询键值
     ↓
   检查内存表 → 未找到
     ↓
   检查不可变内存表 → 未找到
     ↓
   检查布隆过滤器
     ↓
   按层检查SSTable文件
   ```

### 核心组件详解

1. **MemTable（内存表）**
   - 实现：跳表数据结构
   - 特点：快速的读写性能
   - 源码：`pylsm/memtable.py`

2. **WAL（预写日志）**
   - 作用：确保数据持久性
   - 实现：顺序写入磁盘
   - 源码：`pylsm/wal.py`

3. **SSTable（排序字符串表）**
   - 结构：数据块+索引块+元数据
   - 特点：不可变、有序存储
   - 源码：`pylsm/sstable.py`

4. **布隆过滤器**
   - 作用：快速判断键是否存在
   - 原理：概率型数据结构
   - 源码：`pylsm/bloom_filter.py`

## 🔬 高级特性

### 优化配置示例

```python
from pylsm.db import DB, Options

# 创建优化的配置选项
options = Options(
    # 内存表大小设置为2MB
    memtable_size=2 * 1024 * 1024,
    
    # 布隆过滤器参数（每个键使用10位）
    bloom_filter_bits=10,
    
    # 最大层级数（影响压缩策略）
    max_level=7,
    
    # Level 0大小设置为4MB
    level0_size=4 * 1024 * 1024,
    
    # 相邻层大小比例
    size_ratio=10
)

# 使用优化配置创建数据库
db = DB("./optimized_db", options=options)
```

### 批量写入操作

```python
# 原子性批量写入示例
with db.batch_write() as batch:
    for i in range(1000):
        key = f"user:{i}".encode()
        value = f"data:{i}".encode()
        batch.put(key, value)
```

### 高级范围查询

```python
# 带限制的范围查询示例
def range_query_with_limit(db, start_key, end_key, limit=10):
    count = 0
    print(f"查询范围: {start_key.decode()} 到 {end_key.decode()}")
    print("-" * 40)
    
    for key, value in db.range(start_key, end_key):
        print(f"键: {key.decode()}")
        print(f"值: {value.decode()}")
        print("-" * 20)
        
        count += 1
        if count >= limit:
            break
            
    print(f"共返回 {count} 条记录")
```

## 🎯 性能优化技巧

1. **内存管理**
   - 合理设置内存表大小
   - 控制缓存使用量
   - 及时触发内存表刷盘

2. **压缩策略**
   - 选择合适的层级数
   - 设置合理的大小比例
   - 控制文件数量

3. **读取优化**
   - 利用布隆过滤器
   - 缓存热点数据
   - 优化查找路径

## 🔍 调试和监控

### 性能分析工具

```python
from pylsm.db import DB
import time

def benchmark_write(db, count=10000):
    start_time = time.time()
    
    for i in range(count):
        key = f"bench:key:{i}".encode()
        value = f"value:{i}".encode()
        db.put(key, value)
        
    duration = time.time() - start_time
    ops_per_sec = count / duration
    
    print(f"写入 {count} 条记录")
    print(f"总耗时: {duration:.2f} 秒")
    print(f"性能: {ops_per_sec:.2f} ops/sec")
```

### 监控指标

```python
def print_db_stats(db):
    print("数据库状态:")
    print(f"- 内存表大小: {db.memtable_size} bytes")
    print(f"- SSTable文件数: {len(db.sstables)}")
    print(f"- 总记录数: {db.total_keys}")
    print(f"- 布隆过滤器误判率: {db.false_positive_rate:.4f}")
```

## 📝 开发建议

1. **代码风格**
   - 遵循PEP 8规范
   - 添加详细注释
   - 使用类型提示

2. **测试覆盖**
   - 单元测试
   - 集成测试
   - 性能测试

3. **错误处理**
   - 异常捕获
   - 日志记录
   - 优雅降级

## 🤝 参与贡献

欢迎提交Pull Request来改进项目！建议：

1. Fork本仓库
2. 创建特性分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request

## 📄 开源协议

本项目采用MIT协议开源 - 详见 [LICENSE](LICENSE) 文件

