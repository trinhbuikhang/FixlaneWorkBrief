"""
Database Connection Pool Helper
Provides connection pooling for future database support.

Note: Not used at application runtime; only referenced by tests (tests/test_db_pool.py).
Kept for future DB features. Do not bundle as required for main app startup.
"""

import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from queue import Empty, Queue
from typing import Generator, Optional

logger = logging.getLogger(__name__)


class DatabasePool:
    """
    Database connection pool for SQLite.
    
    Manages a pool of database connections to improve performance
    and prevent connection exhaustion.
    
    Example:
        pool = DatabasePool('data.db', pool_size=5)
        
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users")
            results = cursor.fetchall()
        
        pool.close_all()
    """
    
    def __init__(self, db_path: str, pool_size: int = 5):
        """
        Initialize connection pool.
        
        Args:
            db_path: Path to SQLite database file
            pool_size: Maximum number of connections in pool
        """
        self.db_path = Path(db_path)
        self.pool_size = pool_size
        self._pool: Queue = Queue(maxsize=pool_size)
        self._all_connections = []
        self._lock = threading.Lock()
        self._initialized = False
        
        logger.info(f"Initializing connection pool: {db_path}, size={pool_size}")
    
    def _create_connection(self) -> sqlite3.Connection:
        """
        Create a new database connection.
        
        Returns:
            SQLite connection object
        """
        try:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Performance optimizations
            conn.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging
            conn.execute("PRAGMA synchronous = NORMAL")
            
            with self._lock:
                self._all_connections.append(conn)
            
            logger.debug(f"Created new connection: total={len(self._all_connections)}")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            raise
    
    def _initialize_pool(self):
        """Initialize the connection pool with connections"""
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:  # Double-check
                return
            
            for i in range(self.pool_size):
                conn = self._create_connection()
                self._pool.put(conn)
            
            self._initialized = True
            logger.info(f"Connection pool initialized with {self.pool_size} connections")
    
    @contextmanager
    def get_connection(self, timeout: float = 30.0) -> Generator[sqlite3.Connection, None, None]:
        """
        Get a connection from the pool.
        
        Args:
            timeout: Maximum time to wait for available connection (seconds)
        
        Yields:
            Database connection
        
        Raises:
            TimeoutError: If no connection available within timeout
            
        Example:
            with pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM table")
        """
        if not self._initialized:
            self._initialize_pool()
        
        conn = None
        try:
            # Try to get connection from pool
            try:
                conn = self._pool.get(timeout=timeout)
                logger.debug(f"Connection acquired from pool")
            except Empty:
                raise TimeoutError(
                    f"Could not acquire connection within {timeout} seconds. "
                    f"Pool size: {self.pool_size}, consider increasing pool_size."
                )
            
            yield conn
            
        except Exception as e:
            # Rollback on error
            if conn:
                try:
                    conn.rollback()
                    logger.debug("Transaction rolled back due to error")
                except Exception as rollback_err:
                    logger.warning("Rollback failed: %s", rollback_err)
            raise
            
        finally:
            # Return connection to pool
            if conn:
                try:
                    conn.commit()
                except Exception as commit_err:
                    logger.warning("Commit failed when returning connection: %s", commit_err)

                # Return to pool
                self._pool.put(conn)
                logger.debug("Connection returned to pool")
    
    def execute_query(self, query: str, params: tuple = None) -> list:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
        
        Returns:
            List of result rows
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
        
        Returns:
            Number of affected rows
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.rowcount
    
    def execute_many(self, query: str, params_list: list) -> int:
        """
        Execute query with multiple parameter sets.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
        
        Returns:
            Number of affected rows
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            return cursor.rowcount
    
    def get_pool_stats(self) -> dict:
        """
        Get connection pool statistics.
        
        Returns:
            Dictionary with pool statistics
        """
        return {
            'pool_size': self.pool_size,
            'available': self._pool.qsize(),
            'in_use': self.pool_size - self._pool.qsize(),
            'total_connections': len(self._all_connections),
            'database': str(self.db_path)
        }
    
    def close_all(self):
        """Close all connections in the pool"""
        logger.info("Closing all database connections")
        
        with self._lock:
            # Close all connections
            for conn in self._all_connections:
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")
            
            # Clear lists
            self._all_connections.clear()
            
            # Empty queue
            while not self._pool.empty():
                try:
                    self._pool.get_nowait()
                except Empty:
                    break
            
            self._initialized = False
            logger.info("All connections closed")
    
    def __enter__(self):
        """Context manager entry"""
        self._initialize_pool()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_all()
    
    def __del__(self):
        """Destructor - ensure connections are closed"""
        try:
            self.close_all()
        except Exception as e:
            logger.debug("Error in pool destructor: %s", e)


# Convenience function
def create_pool(db_path: str, pool_size: int = 5) -> DatabasePool:
    """
    Create and initialize a database connection pool.
    
    Args:
        db_path: Path to database file
        pool_size: Maximum number of connections
    
    Returns:
        DatabasePool instance
    """
    return DatabasePool(db_path, pool_size)


if __name__ == '__main__':
    # Example usage
    logging.basicConfig(level=logging.DEBUG)
    
    # Create test database
    test_db = 'test_pool.db'
    
    # Create pool
    with DatabasePool(test_db, pool_size=3) as pool:
        # Create table
        pool.execute_update('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )
        ''')
        
        # Insert data
        pool.execute_update(
            "INSERT OR REPLACE INTO users (id, name, email) VALUES (?, ?, ?)",
            (1, 'John Doe', 'john@example.com')
        )
        
        # Bulk insert
        users = [
            (2, 'Jane Smith', 'jane@example.com'),
            (3, 'Bob Johnson', 'bob@example.com')
        ]
        pool.execute_many(
            "INSERT OR REPLACE INTO users (id, name, email) VALUES (?, ?, ?)",
            users
        )
        
        # Query
        results = pool.execute_query("SELECT * FROM users")
        for row in results:
            print(f"User: {row['name']} ({row['email']})")
        
        # Stats
        stats = pool.get_pool_stats()
        print(f"\nPool stats: {stats}")
    
    # Clean up
    import os
    if os.path.exists(test_db):
        os.remove(test_db)
    print("\nTest completed successfully!")
