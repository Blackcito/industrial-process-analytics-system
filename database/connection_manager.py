#connection_manager.py
import mariadb
import logging
from config import credentials
#from config import database_config
class DatabaseConnectionManager:
    def __init__(self):
        self.connections = credentials.DB_CONFIGS
        self.active_connections = {}
        self.logger = logging.getLogger(__name__)

    def connect(self, db_type):
        if db_type not in self.connections:
            raise ValueError(f"Invalid database type: {db_type}")
        
        if db_type in self.active_connections:
            return self.active_connections[db_type]
        
        config = self.connections[db_type]
        try:
            conn = mariadb.connect(**config)
            self.active_connections[db_type] = conn
            self.logger.info(f"Successful connection to {db_type} DB")
            return conn
        except mariadb.Error as e:
            self.logger.error(f"Error connecting to {db_type} DB: {e}")
            return None

    def close_connection(self, db_type):
        if db_type in self.active_connections:
            conn = self.active_connections[db_type]
            if conn:
                conn.close()
            del self.active_connections[db_type]

    def close_all(self):
        for db_type, conn in list(self.active_connections.items()):
            if conn:
                conn.close()
            del self.active_connections[db_type]