"""
Query Executor Module

Provides a clean interface for executing database queries with:
- Automatic connection management
- Transaction support
- Context manager for batch operations
- Error handling and logging
"""

import mariadb
import logging
from contextlib import contextmanager


class QueryExecutor:
    """
    Executes database queries with automatic connection and error handling.
    Supports both single queries and batch operations via context manager.
    """
    
    def __init__(self, connection_manager):
        self.conn_manager = connection_manager
        self.logger = logging.getLogger(__name__)

    def execute_query(self, db_type, query, params=None, fetch_one=False, close_after=True):
        """
        Executes a SELECT query
        
        Args:
            db_type: Database type to query
            query: SQL query string
            params: Query parameters (optional)
            fetch_one: If True, fetch single row; if False, fetch all
            close_after: If True, close connection after query
            
        Returns:
            Query results or None if error
        """
        conn = self.conn_manager.connect(db_type)
        if not conn:
            return None
            
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            return cursor.fetchone() if fetch_one else cursor.fetchall()
        except mariadb.Error as e:
            self.logger.error(f"Query error ({db_type}): {e}")
            return None
        finally:
            if close_after:
                self.conn_manager.close_connection(db_type)

    def execute_update(self, db_type, query, params=None, close_after=True):
        """
        Executes an INSERT/UPDATE/DELETE query
        
        Args:
            db_type: Database type to update
            query: SQL query string
            params: Query parameters (optional)
            close_after: If True, close connection after update
            
        Returns:
            bool: True if successful, False otherwise
        """
        conn = self.conn_manager.connect(db_type)
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
            return True
        except mariadb.Error as e:
            self.logger.error(f"Update error ({db_type}): {e}")
            return False
        finally:
            if close_after:
                self.conn_manager.close_connection(db_type)

    def execute_many(self, db_type, query, params_list, close_after=True):
        """
        Executes multiple queries in batch
        
        Args:
            db_type: Database type to update
            query: SQL query string
            params_list: List of parameter tuples
            close_after: If True, close connection after batch
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not params_list:
            return True
            
        conn = self.conn_manager.connect(db_type)
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return True
        except mariadb.Error as e:
            self.logger.error(f"Batch update error ({db_type}): {e}")
            return False
        finally:
            if close_after:
                self.conn_manager.close_connection(db_type)

    @contextmanager
    def connection(self, db_type, close_after=False):
        """
        Context manager for connection and cursor management
        
        Recommended for processors that perform many sequential operations.
        Automatically commits on success or rolls back on error.
        
        Args:
            db_type: Database type
            close_after: If True, close connection when context exits
            
        Yields:
            tuple: (connection, cursor)
            
        Example:
            with query_executor.connection('analytics') as (conn, cursor):
                cursor.execute(query1, params1)
                cursor.execute(query2, params2)
                # Auto-commits here
        """
        conn = self.conn_manager.connect(db_type)
        if not conn:
            raise RuntimeError(f"Could not connect to {db_type}")
            
        cursor = conn.cursor()
        try:
            yield conn, cursor
            # Commit on successful completion
            try:
                conn.commit()
            except Exception as e:
                self.logger.warning(f"Could not auto-commit: {e}")
        except Exception:
            # Rollback on error
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            if close_after:
                self.conn_manager.close_connection(db_type)