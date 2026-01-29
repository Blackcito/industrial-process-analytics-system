#state_manager.py
from datetime import datetime
import logging

class StateManager:
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.logger = logging.getLogger(__name__)
        self.last_processed_time = self.get_last_processed_time()
        self.logger.info(f"Last processed time: {self.last_processed_time}")

    def get_last_processed_time(self):
        query = "SELECT last_processed_time FROM tb_processing_state WHERE id = 1"
        result = self.query_executor.execute_query('Combined', query, fetch_one=True)
        if result and result[0]:
            if isinstance(result[0], str):
                try:
                    return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            return result[0]
        return None

    def get_last_processed_time_from_conveyor_data(self):
        """
        Obtiene el último tiempo procesado basado en timestamp_conveyor de los datos combinados
        """
        query = "SELECT MAX(timestamp_conveyor) FROM tb_combined_data WHERE timestamp_conveyor IS NOT NULL"
        result = self.query_executor.execute_query('Combined', query, fetch_one=True)
        if result and result[0]:
            # Convertir a datetime si es string
            if isinstance(result[0], str):
                try:
                    return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            return result[0]
        return None

    def get_last_processed_time_from_code_data(self):
        """
        Método original mantenido por compatibilidad
        """
        query = "SELECT MAX(code_timestamp) FROM tb_combined_data"
        result = self.query_executor.execute_query('Combined', query, fetch_one=True)
        if result and result[0]:
            if isinstance(result[0], str):
                try:
                    return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            return result[0]
        return None

    def initialize_from_existing_data(self):
        """
        Inicializa el último tiempo procesado basado en los datos existentes de conveyor
        Si no hay datos de conveyor, usa los datos de código como fallback
        """

        last_conveyor_time = self.get_last_processed_time_from_conveyor_data()
        current_state = self.last_processed_time
        
        if current_state and last_conveyor_time and current_state > last_conveyor_time:
            self.logger.warning(f"Inconsistent state! Resetting to last conveyor data: {last_conveyor_time}")
            self.persist_last_processed_time(last_conveyor_time)
            self.last_processed_time = last_conveyor_time
            return last_conveyor_time


        # Primero intentar con datos de conveyor
        last_conveyor_time = self.get_last_processed_time_from_conveyor_data()
        if last_conveyor_time:
            self.logger.info(f"Initializing from last conveyor record: {last_conveyor_time}")
            self.persist_last_processed_time(last_conveyor_time)
            self.last_processed_time = last_conveyor_time
            return last_conveyor_time
        
        # Fallback a datos de código
        last_code_time = self.get_last_processed_time_from_code_data()
        if last_code_time:
            self.logger.info(f"Initializing from last code record: {last_code_time}")
            self.persist_last_processed_time(last_code_time)
            self.last_processed_time = last_code_time
            return last_code_time
        
        self.logger.info("No existing data found for initialization")
        return None

    def persist_last_processed_time(self, last_time):
        if not last_time:
            return False
        query = """
        INSERT INTO tb_processing_state (id, last_processed_time, updated_at)
        VALUES (1, %s, NOW())
        ON DUPLICATE KEY UPDATE
            last_processed_time = VALUES(last_processed_time),
            updated_at = VALUES(updated_at)
        """
        # Asegurarse de pasar un objeto datetime o string con el formato correcto
        if isinstance(last_time, datetime):
            param = last_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            param = last_time
        return self.query_executor.execute_update('Combined', query, (param,))

    def update_last_processed_time(self, new_time):
        if self.persist_last_processed_time(new_time):
            # Normalizar a datetime si viene como string
            if isinstance(new_time, str):
                try:
                    new_time = datetime.strptime(new_time, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    new_time = datetime.strptime(new_time, '%Y-%m-%d %H:%M:%S.%f')
            self.last_processed_time = new_time
            self.logger.info(f"Time updated: {new_time}")
            return True
        return False

    def get_processing_statistics(self):
        """
        Obtiene estadísticas del procesamiento para debugging
        """
        stats = {}
        
        # Estadísticas de conveyor
        query_conveyor = """
        SELECT 
            COUNT(*) as total_records,
            MIN(timestamp_conveyor) as first_conveyor,
            MAX(timestamp_conveyor) as last_conveyor
        FROM tb_combined_data 
        WHERE timestamp_conveyor IS NOT NULL
        """
        result = self.query_executor.execute_query('Combined', query_conveyor, fetch_one=True)
        if result:
            stats['conveyor_records'] = result[0]
            stats['first_conveyor_time'] = result[1]
            stats['last_conveyor_time'] = result[2]
        
        # Estadísticas generales
        query_general = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT timestamp_conveyor) as unique_conveyor_requests,
            COUNT(DISTINCT code_timestamp) as unique_codes
        FROM tb_combined_data
        """
        result = self.query_executor.execute_query('Combined', query_general, fetch_one=True)
        if result:
            stats['total_records'] = result[0]
            stats['unique_conveyor_requests'] = result[1]
            stats['unique_codes'] = result[2]
        
        return stats