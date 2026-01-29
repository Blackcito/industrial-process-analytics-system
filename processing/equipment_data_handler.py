"""
Equipment Data Handler

Manages equipment controller data operations:
- Retrieving product code descriptions
- Fetching equipment data within time ranges
- Saving combined data (equipment + scanner) to database

This module acts as the bridge between equipment controller readings
and the analytics database.
"""

import logging
from .process_decoder import decode_status_complete, decode_status_current
from database.descriptions_dict import descriptions


class EquipmentDataHandler:
    """
    Handles equipment data operations including description retrieval,
    time-range queries, and combined data persistence.
    """
    
    def __init__(self, query_executor):
        """
        Initializes the equipment data handler
        
        Args:
            query_executor: Database query executor instance
        """
        self.query_executor = query_executor
        self.logger = logging.getLogger(__name__)
        self.descriptions_cache = {}
        
    def get_code_description(self, product_code: str) -> str:
        """
        Retrieves product description from in-memory catalog
        
        Args:
            product_code: Product code to look up
            
        Returns:
            str: Product description or empty string if not found
        """
        description = descriptions.get(product_code, "")
        self.descriptions_cache[product_code] = description
        return description

    def get_equipment_data_by_time_range(self, start_time, end_time):
        """
        Retrieves equipment data within a specific time range
        
        Args:
            start_time (datetime): Range start time
            end_time (datetime): Range end time
            
        Returns:
            dict: Dictionary containing status_field records
        """
        query = """
            SELECT status_field, TIMESTAMP(date_field, time_field) as equipment_timestamp
            FROM tb_equipment_records
            WHERE TIMESTAMP(date_field, time_field) > %s 
            AND TIMESTAMP(date_field, time_field) <= %s
            ORDER BY TIMESTAMP(date_field, time_field) ASC
        """
        
        status_records = self.query_executor.execute_query(
            'equipment', query, (start_time, end_time)
        ) or []
        
        return {'status_records': status_records}

    def save_combined_data_centered_conveyor(self, combined_data):
        """
        Saves combined data (equipment + scanner) to the database
        
        This method correlates equipment controller data with product scanner
        data based on conveyor timestamps.
        
        Args:
            combined_data (dict): Dictionary containing:
                - conveyor_time: Conveyor request timestamp
                - code_data: Scanner data tuple
                - equipment_data: Equipment controller data
                
        Returns:
            bool: True if saved successfully, False otherwise
        """
        conveyor_time = combined_data['conveyor_time']
        code_data = combined_data['code_data']
        equipment_data = combined_data['equipment_data']
        
        status_records = equipment_data['status_records']
        
        if not status_records:
            self.logger.info("No equipment records to save")
            return True
        
        # Extract scanner data
        scanner_timestamp = code_data[0]
        product_code = code_data[1]
        operator_id = code_data[2]
        work_order = code_data[3]
        
        # Get product description
        code_description = self.get_code_description(product_code)
        
        query = """
            INSERT IGNORE INTO tb_combined_data
            (scanner_timestamp, equipment_timestamp, conveyor_timestamp, 
             status_field, process_status, process_complete_status, 
             product_code, code_description, operator_id, work_order)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        params = []
        for equipment_record in status_records:
            status_value = equipment_record[0]
            equipment_timestamp = equipment_record[1]
            
            process_status = decode_status_current(status_value)
            process_complete_status = decode_status_complete(status_value)
            
            params.append((
                scanner_timestamp,
                equipment_timestamp,
                conveyor_time,
                status_value,
                process_status,
                process_complete_status,
                product_code,
                code_description,
                operator_id,
                work_order
            ))
        
        success = self.query_executor.execute_many('analytics', query, params)
        
        if success:
            self.logger.info(
                f"Saved {len(params)} records with description for conveyor: {conveyor_time}"
            )
        else:
            self.logger.error(f"Error saving records for conveyor: {conveyor_time}")
        
        return success