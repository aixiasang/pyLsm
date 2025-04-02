#!/usr/bin/env python3
"""
运行所有PyLSM数据库测试。
"""

import unittest
import sys
import os
import argparse

# 添加父目录到路径，以便可以导入pylsm模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def run_tests(pattern=None, verbose=False):
    """
    运行测试套件。
    
    Args:
        pattern: 用于匹配测试模块的模式，例如 "test_memtable"
        verbose: 是否显示详细输出
    """
    # 发现测试用例
    if pattern:
        test_suite = unittest.defaultTestLoader.discover(
            start_dir=os.path.dirname(__file__),
            pattern=f"{pattern}*.py"
        )
    else:
        test_suite = unittest.defaultTestLoader.discover(
            start_dir=os.path.dirname(__file__),
            pattern="test_*.py"
        )
    
    # 运行测试
    verbosity = 2 if verbose else 1
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(test_suite)
    
    # 返回状态码，如果有测试失败则为非零
    return not result.wasSuccessful()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="运行PyLSM数据库测试")
    parser.add_argument(
        "-p", "--pattern", 
        help="用于匹配测试模块的模式，例如 'bloom_filter' 将运行 test_bloom_filter.py"
    )
    parser.add_argument(
        "-v", "--verbose", 
        action="store_true",
        help="显示详细输出"
    )
    
    args = parser.parse_args()
    sys.exit(run_tests(args.pattern, args.verbose)) 