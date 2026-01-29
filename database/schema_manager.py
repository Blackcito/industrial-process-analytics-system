#schema_manager.py
import logging
import mariadb

class SchemaManager:
    def __init__(self, connection_manager):
        self.conn_manager = connection_manager
        self.logger = logging.getLogger(__name__)

    def create_combined_table(self):
        """Crea la tabla principal de datos combinados"""
        return self._create_table('Combined', """
            CREATE TABLE IF NOT EXISTS tb_combined_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code_timestamp DATETIME NOT NULL,
                timestamp_equipment DATETIME NOT NULL,
                timestamp_conveyor DATETIME NOT NULL,
                field_24 FLOAT,
                v24_description VARCHAR(255),
                is_completed TEXT,
                product_code VARCHAR(255),
                code_description TEXT, 
                operator_code VARCHAR(255),
                order_id VARCHAR(255),
                INDEX idx_code (code_timestamp),
                INDEX idx_equipment_time (timestamp_equipment),
                INDEX idx_v24_description (v24_description),
                UNIQUE KEY uq_code_equipment (code_timestamp, timestamp_equipment, timestamp_conveyor)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

    def create_processing_state_table(self):
        """Crea la tabla de estado del procesamiento"""
        return self._create_table('Combined', """
            CREATE TABLE IF NOT EXISTS tb_processing_state (
                id INT PRIMARY KEY DEFAULT 1,
                last_processed_time DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
        """)

    def create_analytics_tables(self):
        """Crea todas las tablas necesarias para analíticas"""
        tables_created = 0
        
        # Tabla principal de estadísticas por process individual
        if self.create_process_statistics_table():
            tables_created += 1
            
        # Tabla agregada por día
        if self.create_daily_statistics_table():
            tables_created += 1
            
        # Tabla agregada por tipo de colchón
        if self.create_product_statistics_table():
            tables_created += 1
            
        # Tabla agregada por operator
        if self.create_operator_statistics_table():
            tables_created += 1

        self.logger.info(f"Tablas de analytics creadas/verificadas: {tables_created}/4")
        return tables_created == 4

    def create_process_statistics_table(self):
        """Crea la tabla de estadísticas por process individual con todas las columnas necesarias"""
        return self._create_table('Combined', """
            CREATE TABLE IF NOT EXISTS tb_process_statistics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code_timestamp DATETIME NOT NULL,
                process_date DATE NOT NULL,
                start_time TIME NOT NULL,
                product_code VARCHAR(255) NOT NULL,
                code_description TEXT,
                operator_code VARCHAR(255),
                order_id VARCHAR(255),

                /* Tiempos generales */
                total_time_minutes DECIMAL(10,2) NOT NULL,
                production_time_minutes DECIMAL(10,2) DEFAULT 0, /* Solo Cara A a Cara B */
                equipment_records_count INT DEFAULT 0,

                /* Tiempos entre processes */
                wait_time_minutes DECIMAL(10,2) DEFAULT 0,

                /* Tiempos de preparación */
                conveyor_to_code_minutes DECIMAL(10,2) DEFAULT 0,
                conveyor_to_equipment_minutes DECIMAL(10,2) DEFAULT 0,
                code_to_start_minutes DECIMAL(10,2) DEFAULT 0,

                /* Tiempos de process */
                time_side_a_minutes DECIMAL(10,2) DEFAULT 0,
                auto_flip_time_minutes DECIMAL(10,2) DEFAULT 0,
                manual_flip_time_minutes DECIMAL(10,2) DEFAULT 0,
                time_side_b_minutes DECIMAL(10,2) DEFAULT 0,
                final_time_minutes DECIMAL(10,2) DEFAULT 0,

                /* Nuevos campos: tiempos pre y post producción */
                pre_production_time_minutes DECIMAL(10,2) DEFAULT 0,
                post_production_time_minutes DECIMAL(10,2) DEFAULT 0,

                /* Campos de validación y control */
                is_incomplete TINYINT(1) DEFAULT 0, /* 1 si faltan marcas */
                is_discarded TINYINT(1) DEFAULT 0, /* 1 si total > 30 min */

                /* Auditoría */
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                /* Índices */
                INDEX idx_date (process_date),
                INDEX idx_product (product_code),
                INDEX idx_operator (operator_code),
                INDEX idx_order (order_id),
                INDEX idx_date_time (process_date, start_time),
                UNIQUE KEY uq_process_time (product_code, process_date, start_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

    def create_daily_statistics_table(self):
        """Crea la tabla de estadísticas agregadas por día con nuevos campos"""
        return self._create_table('Combined', """
            CREATE TABLE IF NOT EXISTS tb_daily_statistics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL UNIQUE,
                total_processes INT DEFAULT 0,
                average_time_minutes DECIMAL(10,2) DEFAULT 0,
                max_time_minutes DECIMAL(10,2) DEFAULT 0,
                min_time_minutes DECIMAL(10,2) DEFAULT 0,
                standard_deviation DECIMAL(10,2) DEFAULT 0,
                completed_processes INT DEFAULT 0,
                incomplete_processes INT DEFAULT 0,
                average_efficiency DECIMAL(5,2) DEFAULT 0,
                # Nuevos campos para tiempos de espera
                average_wait_time_seconds DECIMAL(10,2) DEFAULT 0,
                max_wait_time_seconds DECIMAL(10,2) DEFAULT 0,
                min_wait_time_seconds DECIMAL(10,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_date (date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

    def create_product_statistics_table(self):
        """Crea la tabla de estadísticas agregadas por producto"""
        return self._create_table('Combined', """
            CREATE TABLE IF NOT EXISTS tb_product_statistics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_code VARCHAR(255) NOT NULL,
                code_description TEXT,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                total_processes INT DEFAULT 0,
                average_total_time_minutes DECIMAL(10,2) DEFAULT 0,
                average_production_time_minutes DECIMAL(10,2) DEFAULT 0,
                max_total_time_minutes DECIMAL(10,2) DEFAULT 0,
                min_total_time_minutes DECIMAL(10,2) DEFAULT 0,
                standard_deviation DECIMAL(10,2) DEFAULT 0,
                average_efficiency DECIMAL(5,2) DEFAULT 0,
                products_per_hour DECIMAL(8,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_product_period (product_code, start_date, end_date),
                INDEX idx_product (product_code),
                INDEX idx_period (start_date, end_date),
                INDEX idx_efficiency (average_efficiency)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

    def create_operator_statistics_table(self):
        """Crea la tabla de estadísticas agregadas por operator"""
        return self._create_table('Combined', """
            CREATE TABLE IF NOT EXISTS tb_operator_statistics (
                id INT AUTO_INCREMENT PRIMARY KEY,
                operator_code VARCHAR(255) NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                total_processes INT DEFAULT 0,
                average_time_minutes DECIMAL(10,2) DEFAULT 0,
                max_time_minutes DECIMAL(10,2) DEFAULT 0,
                min_time_minutes DECIMAL(10,2) DEFAULT 0,
                standard_deviation DECIMAL(10,2) DEFAULT 0,
                average_efficiency DECIMAL(5,2) DEFAULT 0,
                different_products INT DEFAULT 0,
                processes_per_hour DECIMAL(8,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_operator_period (operator_code, start_date, end_date),
                INDEX idx_operator (operator_code),
                INDEX idx_period (start_date, end_date),
                INDEX idx_efficiency (average_efficiency)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

    def create_analytics_control_table(self):
        """Crea tabla de control para el procesamiento de analytics"""
        return self._create_table('Combined', """
            CREATE TABLE IF NOT EXISTS tb_analytics_control (
                id INT PRIMARY KEY DEFAULT 1,
                last_individual_processing DATETIME NULL,
                last_daily_processing DATETIME NULL,
                last_product_processing DATETIME NULL,
                last_operator_processing DATETIME NULL,
                pending_processes INT DEFAULT 0,
                last_error TEXT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

    def create_table_descriptions(cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS code_descriptions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_code VARCHAR(20),
                code_description TEXT,
                UNIQUE (product_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)

    def create_all_tables(self):
        """Crea todas las tablas del sistema"""
        tables_created = 0
        tables = [
            ("Combined Data", self.create_combined_table),
            ("Processing State", self.create_processing_state_table),
            ("Process Statistics", self.create_process_statistics_table),
            ("Estadísticas Diarias", self.create_daily_statistics_table),
            ("Estadísticas por Producto", self.create_product_statistics_table),
            ("Estadísticas por Operator", self.create_operator_statistics_table),
            ("Control Analytics", self.create_analytics_control_table),
            ("Nombres codigo", self.create_table_descriptions)
        ]
        
        for table_name, create_method in tables:
            try:
                if create_method():
                    tables_created += 1
                    self.logger.info(f"✓ Tabla creada/verificada: {table_name}")
                else:
                    self.logger.error(f"✗ Error creando tabla: {table_name}")
            except Exception as e:
                self.logger.error(f"✗ Excepción creando tabla {table_name}: {e}")
        
        self.logger.info(f"Resumen: {tables_created}/{len(tables)} tablas creadas/verificadas exitosamente")
        return tables_created == len(tables)

    def _create_table(self, db_type, query):
        """Método auxiliar para crear tablas con manejo mejorado de errores"""
        conn = self.conn_manager.connect(db_type)
        if not conn:
            self.logger.error(f"No se pudo conectar a la base de datos {db_type}")
            return False
            
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return True
        except mariadb.Error as e:
            self.logger.error(f"Error creando tabla en {db_type} DB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado creando tabla: {e}")
            return False
        finally:
            self.conn_manager.close_connection(db_type)

    def table_exists(self, table_name, db_type='Combined'):
        conn = self.conn_manager.connect(db_type)
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
                AND table_name = %s
            """, (table_name,))
            return cursor.fetchone()[0] > 0
        except Exception:
            return False
        finally:
            self.conn_manager.close_connection(db_type)


    def verify_analytics_schema(self):
        """Verifica que todas las tablas de analytics existan y tengan la estructura correcta"""
        verification_queries = {
            'tb_process_statistics': """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'tb_process_statistics' 
                AND TABLE_SCHEMA = DATABASE()
            """,
            'tb_daily_statistics': """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'tb_daily_statistics' 
                AND TABLE_SCHEMA = DATABASE()
            """,
            'tb_product_statistics': """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'tb_product_statistics' 
                AND TABLE_SCHEMA = DATABASE()
            """,
            'tb_operator_statistics': """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'tb_operator_statistics' 
                AND TABLE_SCHEMA = DATABASE()
            """
        }
        
        conn = self.conn_manager.connect('Combined')
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            all_valid = True
            
            for table_name, query in verification_queries.items():
                cursor.execute(query)
                result = cursor.fetchone()
                column_count = result[0] if result else 0
                
                if column_count == 0:
                    self.logger.warning(f"Tabla {table_name} no existe o no tiene columnas")
                    all_valid = False
                else:
                    self.logger.info(f"Tabla {table_name} verificada: {column_count} columnas")
            
            return all_valid
            
        except mariadb.Error as e:
            self.logger.error(f"Error verificando esquema de analytics: {e}")
            return False
        finally:
            self.conn_manager.close_connection('Combined')

    def get_table_sizes(self):
        """Obtiene información sobre el tamaño de las tablas para monitoreo"""
        size_query = """
        SELECT 
            TABLE_NAME,
            TABLE_ROWS,
            ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS 'Size_MB'
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME IN (
            'tb_combined_data',
            'tb_process_statistics', 
            'tb_daily_statistics',
            'tb_product_statistics',
            'tb_operator_statistics'
        )
        ORDER BY Size_MB DESC
        """
        
        conn = self.conn_manager.connect('Combined')
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute(size_query)
            results = cursor.fetchall()
            
            table_info = {}
            for table_name, row_count, size_mb in results:
                table_info[table_name] = {
                    'rows': row_count or 0,
                    'size_mb': size_mb or 0
                }
            
            return table_info
            
        except mariadb.Error as e:
            self.logger.error(f"Error obteniendo tamaños de tabla: {e}")
            return None
        finally:
            self.conn_manager.close_connection('Combined')

    def optimize_analytics_tables(self):
        """Optimiza las tablas de analytics para mejor rendimiento"""
        tables_to_optimize = [
            'tb_process_statistics',
            'tb_daily_statistics', 
            'tb_product_statistics',
            'tb_operator_statistics'
        ]
        
        conn = self.conn_manager.connect('Combined')
        if not conn:
            return False
        
        optimized_count = 0
        
        try:
            cursor = conn.cursor()
            
            for table_name in tables_to_optimize:
                try:
                    self.logger.info(f"Optimizando tabla {table_name}...")
                    cursor.execute(f"OPTIMIZE TABLE {table_name}")
                    optimized_count += 1
                    self.logger.info(f"✓ Tabla {table_name} optimizada")
                except mariadb.Error as e:
                    self.logger.error(f"Error optimizando tabla {table_name}: {e}")
                    continue
            
            self.logger.info(f"Optimización completada: {optimized_count}/{len(tables_to_optimize)} tablas")
            return optimized_count == len(tables_to_optimize)
            
        except Exception as e:
            self.logger.error(f"Error durante optimización: {e}")
            return False
        finally:
            self.conn_manager.close_connection('Combined')