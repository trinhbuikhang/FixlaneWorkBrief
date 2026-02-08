"""
Test Database Connection Pool - Simplified Version
Tests for utils/db_pool.py
"""

import pytest
import sqlite3
from pathlib import Path

from utils.db_pool import create_pool, DatabasePool


class TestDatabasePool:
    """Test suite for DatabasePool class"""
    
    @pytest.fixture
    def test_db(self, tmp_path):
        """Create temporary test database"""
        db_path = tmp_path / "test.db"
        yield str(db_path)
    
    def test_pool_initialization(self, test_db):
        """Test pool initialization"""
        pool = DatabasePool(test_db, pool_size=3)
        assert pool.pool_size == 3
        assert pool.db_path == Path(test_db)
        pool.close_all()
    
    def test_create_connection(self, test_db):
        """Test connection creation"""
        pool = DatabasePool(test_db, pool_size=2)
        with pool.get_connection() as conn:
            assert conn is not None
            assert isinstance(conn, sqlite3.Connection)
        pool.close_all()
    
    def test_connection_reuse(self, test_db):
        """Test that connections are reused from pool"""
        pool = DatabasePool(test_db, pool_size=2)
        
        # Get and release connection
        with pool.get_connection() as conn1:
            conn1_id = id(conn1)
        
        # Get connection again - should be same
        with pool.get_connection() as conn2:
            conn2_id = id(conn2)
        
        # Should reuse same connection
        assert conn1_id == conn2_id
        pool.close_all()
    
    def test_execute_query(self, test_db):
        """Test executing SELECT queries"""
        pool = DatabasePool(test_db, pool_size=2)
        
        pool.execute_update("CREATE TABLE test (id INTEGER, name TEXT)")
        pool.execute_update("INSERT INTO test VALUES (1, 'Test')")
        
        results = pool.execute_query("SELECT * FROM test")
        
        assert len(results) == 1
        assert results[0]['id'] == 1
        assert results[0]['name'] == 'Test'
        pool.close_all()
    
    def test_execute_update(self, test_db):
        """Test executing INSERT/UPDATE/DELETE"""
        pool = DatabasePool(test_db, pool_size=2)
        
        pool.execute_update("CREATE TABLE test (id INTEGER, name TEXT)")
        rows = pool.execute_update("INSERT INTO test VALUES (1, 'Test')")
        assert rows == 1
        
        rows = pool.execute_update("UPDATE test SET name = 'Updated' WHERE id = 1")
        assert rows == 1
        
        results = pool.execute_query("SELECT * FROM test WHERE id = 1")
        assert results[0]['name'] == 'Updated'
        pool.close_all()
    
    def test_execute_many(self, test_db):
        """Test bulk insert"""
        pool = DatabasePool(test_db, pool_size=2)
        pool.execute_update("CREATE TABLE test (id INTEGER, name TEXT)")
        
        data = [(1, 'One'), (2, 'Two'), (3, 'Three')]
        rows = pool.execute_many("INSERT INTO test VALUES (?, ?)", data)
        assert rows == 3
        
        results = pool.execute_query("SELECT * FROM test ORDER BY id")
        assert len(results) == 3
        assert results[1]['name'] == 'Two'
        pool.close_all()
    
    def test_pool_stats(self, test_db):
        """Test pool statistics"""
        pool = DatabasePool(test_db, pool_size=3)
        pool._initialize_pool()
        
        stats = pool.get_pool_stats()
        assert stats['pool_size'] == 3
        assert stats['available'] == 3
        assert 'database' in stats
        pool.close_all()
    
    def test_context_manager(self, test_db):
        """Test using pool as context manager"""
        with DatabasePool(test_db, pool_size=2) as pool:
            pool.execute_update("CREATE TABLE test (id INTEGER)")
            pool.execute_update("INSERT INTO test VALUES (1)")
            results = pool.execute_query("SELECT * FROM test")
            assert len(results) == 1
    
    def test_create_pool_function(self, test_db):
        """Test convenience create_pool function"""
        pool = create_pool(test_db, pool_size=2)
        assert pool.pool_size == 2
        assert isinstance(pool, DatabasePool)
        pool.close_all()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
