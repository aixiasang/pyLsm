"""
Command Line Interface for PyLSM database
"""
import argparse
import os
import sys
import time
from typing import Dict, List, Optional

from pylsm.db import DB


def str_to_bytes(s: str) -> bytes:
    """Convert string to bytes."""
    return s.encode('utf-8')


def bytes_to_str(b: bytes) -> str:
    """Convert bytes to string."""
    return b.decode('utf-8', errors='replace')


class PyLSMCLI:
    """
    Command Line Interface for PyLSM database.
    """
    def __init__(self, db_path: str):
        """
        Initialize the CLI with a database path.
        
        Args:
            db_path: Path to the database directory
        """
        self.db_path = db_path
        self.db = None
        self.commands = {
            'open': self.cmd_open,
            'close': self.cmd_close,
            'put': self.cmd_put,
            'get': self.cmd_get,
            'delete': self.cmd_delete,
            'scan': self.cmd_scan,
            'compact': self.cmd_compact,
            'info': self.cmd_info,
            'benchmark': self.cmd_benchmark,
            'exit': self.cmd_exit,
            'help': self.cmd_help,
        }
    
    def _ensure_db_open(self) -> bool:
        """Ensure the database is open."""
        if self.db is None:
            print("Error: Database not open. Use 'open' command first.")
            return False
        return True
    
    def cmd_open(self, args: List[str]) -> None:
        """Open the database."""
        if self.db is not None:
            print("Database already open. Close it first.")
            return
            
        # 解析参数
        db_path = self.db_path
        if args and args[0] != '--no-create':
            db_path = args[0]
            
        try:
            start_time = time.time()
            self.db = DB(db_path)
            elapsed = time.time() - start_time
            print(f"Database opened in {elapsed:.3f} seconds: {db_path}")
        except Exception as e:
            print(f"Error opening database: {e}")
    
    def cmd_close(self, args: List[str]) -> None:
        """Close the database."""
        if not self._ensure_db_open():
            return
            
        try:
            start_time = time.time()
            self.db.close()
            elapsed = time.time() - start_time
            print(f"Database closed in {elapsed:.3f} seconds")
            self.db = None
        except Exception as e:
            print(f"Error closing database: {e}")
    
    def cmd_put(self, args: List[str]) -> None:
        """Add a key-value pair to the database."""
        if not self._ensure_db_open() or len(args) < 2:
            if len(args) < 2:
                print("Usage: put <key> <value>")
            return
            
        key, value = args[0], ' '.join(args[1:])
        try:
            start_time = time.time()
            self.db.put(str_to_bytes(key), str_to_bytes(value))
            elapsed = time.time() - start_time
            print(f"Put '{key}' in {elapsed:.6f} seconds")
        except Exception as e:
            print(f"Error putting key-value: {e}")
    
    def cmd_get(self, args: List[str]) -> None:
        """Get a value from the database."""
        if not self._ensure_db_open() or len(args) != 1:
            if len(args) != 1:
                print("Usage: get <key>")
            return
            
        key = args[0]
        try:
            start_time = time.time()
            value = self.db.get(str_to_bytes(key))
            elapsed = time.time() - start_time
            
            if value is not None:
                print(f"Got '{key}': '{bytes_to_str(value)}' in {elapsed:.6f} seconds")
            else:
                print(f"Key '{key}' not found in {elapsed:.6f} seconds")
        except Exception as e:
            print(f"Error getting key: {e}")
    
    def cmd_delete(self, args: List[str]) -> None:
        """Delete a key from the database."""
        if not self._ensure_db_open() or len(args) != 1:
            if len(args) != 1:
                print("Usage: delete <key>")
            return
            
        key = args[0]
        try:
            start_time = time.time()
            self.db.delete(str_to_bytes(key))
            elapsed = time.time() - start_time
            print(f"Deleted '{key}' in {elapsed:.6f} seconds")
        except Exception as e:
            print(f"Error deleting key: {e}")
    
    def cmd_scan(self, args: List[str]) -> None:
        """Scan keys in the database."""
        if not self._ensure_db_open():
            return
            
        start_key = None
        end_key = None
        limit = 10  # Default limit
        
        # Parse arguments
        i = 0
        while i < len(args):
            if args[i] == '--start' and i + 1 < len(args):
                start_key = args[i + 1]
                i += 2
            elif args[i] == '--end' and i + 1 < len(args):
                end_key = args[i + 1]
                i += 2
            elif args[i] == '--limit' and i + 1 < len(args):
                try:
                    limit = int(args[i + 1])
                    i += 2
                except ValueError:
                    print(f"Invalid limit: {args[i + 1]}")
                    return
            else:
                print(f"Unknown scan argument: {args[i]}")
                print("Usage: scan [--start <start_key>] [--end <end_key>] [--limit <limit>]")
                return
        
        try:
            start_time = time.time()
            count = 0
            
            for key, value in self.db.range(start_key, end_key):
                print(f"  {key}: {bytes_to_str(value)}")
                count += 1
                if count >= limit:
                    print(f"  (limit of {limit} reached)")
                    break
            
            elapsed = time.time() - start_time
            print(f"Scanned {count} keys in {elapsed:.6f} seconds")
        except Exception as e:
            print(f"Error scanning keys: {e}")
    
    def cmd_compact(self, args: List[str]) -> None:
        """Force a compaction of the database."""
        if not self._ensure_db_open():
            return
            
        try:
            start_time = time.time()
            self.db.compact()
            elapsed = time.time() - start_time
            print(f"Compaction completed in {elapsed:.3f} seconds")
        except Exception as e:
            print(f"Error compacting database: {e}")
    
    def cmd_info(self, args: List[str]) -> None:
        """Show database information."""
        if not self._ensure_db_open():
            return
            
        try:
            # 获取SSTable文件
            sstable_files = []
            for filename in os.listdir(self.db_path):
                if filename.endswith('.sst'):
                    file_path = os.path.join(self.db_path, filename)
                    file_size = os.path.getsize(file_path)
                    sstable_files.append((filename, file_size))
            
            # 打印基本信息
            print(f"Database path: {self.db_path}")
            print(f"Current memtable size: {self.db.memtable.size} bytes")
            
            print(f"\nSSTable files ({len(sstable_files)}):")
            total_size = 0
            for filename, size in sorted(sstable_files):
                total_size += size
                print(f"  {filename}: {size} bytes")
            
            if sstable_files:
                print(f"\nTotal SSTable size: {total_size} bytes ({total_size/1024/1024:.2f} MB)")
                print(f"Average file size: {total_size/len(sstable_files)} bytes")
            
            # 尝试获取版本信息
            try:
                current_version = self.db.version_set.current_version_number
                print(f"\nCurrent version: {current_version}")
            except:
                pass
                
        except Exception as e:
            print(f"Error getting database info: {e}")
    
    def cmd_benchmark(self, args: List[str]) -> None:
        """Run a simple benchmark."""
        if not self._ensure_db_open():
            return
            
        try:
            count = 10000  # Default
            value_size = 100  # Default
            
            # Parse arguments
            i = 0
            while i < len(args):
                if args[i] == '--count' and i + 1 < len(args):
                    try:
                        count = int(args[i + 1])
                        i += 2
                    except ValueError:
                        print(f"Invalid count: {args[i + 1]}")
                        return
                elif args[i] == '--value-size' and i + 1 < len(args):
                    try:
                        value_size = int(args[i + 1])
                        i += 2
                    except ValueError:
                        print(f"Invalid value size: {args[i + 1]}")
                        return
                else:
                    print(f"Unknown benchmark argument: {args[i]}")
                    print("Usage: benchmark [--count <count>] [--value-size <value_size>]")
                    return
            
            # Create test data
            value = b'x' * value_size
            
            # Write benchmark
            print(f"Running write benchmark with {count} keys, {value_size} bytes per value...")
            start_time = time.time()
            
            for i in range(count):
                key = f"bench-{i:08}".encode('utf-8')
                self.db.put(key, value)
                
                if i > 0 and i % 1000 == 0:
                    elapsed = time.time() - start_time
                    print(f"  {i} writes in {elapsed:.3f} seconds, {i/elapsed:.1f} writes/sec")
            
            write_elapsed = time.time() - start_time
            
            # Read benchmark
            print(f"Running read benchmark with {count} keys...")
            start_time = time.time()
            
            hits = 0
            for i in range(count):
                key = f"bench-{i:08}".encode('utf-8')
                value = self.db.get(key)
                if value is not None:
                    hits += 1
                
                if i > 0 and i % 1000 == 0:
                    elapsed = time.time() - start_time
                    print(f"  {i} reads in {elapsed:.3f} seconds, {i/elapsed:.1f} reads/sec")
            
            read_elapsed = time.time() - start_time
            
            # Print results
            print("\nBenchmark results:")
            print(f"  Writes: {count} in {write_elapsed:.3f} seconds, {count/write_elapsed:.1f} writes/sec")
            print(f"  Reads: {count} in {read_elapsed:.3f} seconds, {count/read_elapsed:.1f} reads/sec")
            print(f"  Read hits: {hits}/{count} ({hits/count*100:.1f}%)")
            
        except Exception as e:
            print(f"Error running benchmark: {e}")
    
    def cmd_exit(self, args: List[str]) -> None:
        """Exit the CLI."""
        if self.db is not None:
            self.cmd_close([])
        print("Exiting PyLSM CLI")
        sys.exit(0)
    
    def cmd_help(self, args: List[str]) -> None:
        """Show help information."""
        print("PyLSM CLI Commands:")
        print("  open [--no-create]          Open the database")
        print("  close                       Close the database")
        print("  put <key> <value>           Add a key-value pair")
        print("  get <key>                   Get a value for a key")
        print("  delete <key>                Delete a key")
        print("  scan [--start <key>] [--end <key>] [--limit <n>]")
        print("                              Scan keys in range")
        print("  compact                     Force a compaction")
        print("  info                        Show database information")
        print("  benchmark [--count <n>] [--value-size <bytes>]")
        print("                              Run a simple benchmark")
        print("  exit                        Exit the CLI")
        print("  help                        Show this help")
    
    def run_interactive(self) -> None:
        """Run the interactive CLI loop."""
        print("Type 'help' for a list of commands")
        
        try:
            while True:
                try:
                    command_line = input("pylsm> ").strip()
                    
                    # Skip empty lines
                    if not command_line:
                        continue
                    
                    # Split the command line
                    parts = command_line.split()
                    command = parts[0].lower()
                    args = parts[1:] if len(parts) > 1 else []
                    
                    # Handle the command
                    if command in self.commands:
                        self.commands[command](args)
                    else:
                        print(f"Unknown command: {command}")
                        print("Type 'help' for a list of commands")
                except KeyboardInterrupt:
                    print("\nUse 'exit' to exit the CLI")
                    continue
                except EOFError:
                    print("\nGoodbye!")
                    break
                except Exception as e:
                    print(f"Error: {e}")
        finally:
            # 确保在退出时关闭数据库
            if self.db is not None:
                try:
                    self.db.close()
                    print("Database closed.")
                except:
                    pass


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="PyLSM Database CLI")
    parser.add_argument('db_path', type=str, nargs='?', default='./pylsm_data',
                      help='Path to the database directory')
    
    args = parser.parse_args()
    
    cli = PyLSMCLI(args.db_path)
    cli.run_interactive()


if __name__ == '__main__':
    main() 