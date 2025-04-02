"""
PyLSM - 基于LSM树的轻量级键值存储引擎

PyLSM提供了一个高效的、可配置的键值存储引擎，基于Log-Structured Merge Tree构建。
主要特点包括高写入吞吐量、范围查询支持和丰富的配置选项。

基本用法:
    >>> from pylsm.db import DB
    >>> db = DB("path/to/db")
    >>> db.put("key", "value")
    >>> value = db.get("key")
    >>> for k, v in db.range("a", "z"):
    ...     print(k, v)
    >>> db.close()
"""

__version__ = "0.1.0"

from pylsm.db import DB
from pylsm.config import (
    Config, 
    default_config,
    optimize_for_point_lookup,
    optimize_for_heavy_writes,
    optimize_for_range_scan
) 