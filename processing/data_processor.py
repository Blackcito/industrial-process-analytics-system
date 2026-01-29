#data_processor.py
from datetime import datetime, timedelta
import logging


class DataProcessor:
    """
    Data processor that coordinates:
    - Retrieval of conveyor requests
    - Processing of individual requests
    - Search for corresponding codes
    - Verification of complete cycles
    - Storage of combined data
    """
    
    def __init__(self, query_executor, state_manager, equipment_handler):
        """
        Initializes the data processor
        
        Args:
            query_executor: Database query executor
            state_manager: System state manager
            equipment_handler: Equipment data handler
        """
        self.query_executor = query_executor
        self.state_manager = state_manager
        self.equipment_handler = equipment_handler
        self.logger = logging.getLogger(__name__)
        self.requests_without_code = set()  # Track requests without code

    def get_new_conveyor_requests(self):
        """
        Retrieves new conveyor requests from the database
        
        Returns:
            list: List of filtered conveyor requests
        """
        last_processed = self.state_manager.last_processed_time
        last_dt = None
        if last_processed is not None:
            last_dt = self._convert_to_datetime(last_processed)

        query, params = self._build_conveyor_query()

        try:
            rows = self.query_executor.execute_query('equipment', query, params) or []
            
            if last_dt is not None:
                filtered = [row for row in rows if self._convert_to_datetime(row[0]) >= last_dt]
            else:
                filtered = rows

            self.logger.info(
                f"Found {len(rows)} requests (raw), "
                f"{len(filtered)} after filtering by last_processed_time"
            )
            return filtered
        except Exception as e:
            self.logger.error(f"Error getting conveyor requests: {e}")
            return []

    def process_new_conveyor_requests(self, conveyor_requests):
        """
        Processes the newly obtained conveyor requests
        
        Args:
            conveyor_requests (list): List of conveyor requests to process
        """
        if not conveyor_requests:
            self.logger.info("No new conveyor requests to process")
            return

        self.logger.info(f"Processing {len(conveyor_requests)} conveyor requests...")
        
        for index, conveyor_request in enumerate(conveyor_requests):
            self._process_individual_conveyor_request(
                conveyor_request, conveyor_requests, index
            )

    def _build_conveyor_query(self):
        """
        Builds the SQL query to retrieve conveyor requests
        
        Returns:
            tuple: (query, parameters) for execution
        """
        if self.state_manager.last_processed_time is None:
            query = """
                SELECT code_timestamp
                FROM tb_conveyor_requests
                WHERE DATE(code_timestamp) = CURDATE()
                ORDER BY code_timestamp ASC
            """
            params = ()
            self.logger.info("Searching for conveyor requests from start of day")
        else:
            last_time = self._convert_to_datetime(self.state_manager.last_processed_time)
            search_time = last_time - timedelta(minutes=5)

            query = """
                SELECT code_timestamp
                FROM tb_conveyor_requests
                WHERE code_timestamp > %s
                ORDER BY code_timestamp ASC
            """
            params = (search_time,)
            self.logger.info(f"Searching for conveyor requests after: {search_time}")

        return query, params

    def _verify_complete_cycle(self, conveyor_time, next_conveyor_time=None):
        """
        Verifies if the cycle associated with a conveyor mark is complete
        
        Args:
            conveyor_time (datetime): Time of the conveyor request
            next_conveyor_time (datetime, optional): Time of the next request
            
        Returns:
            bool: True if cycle is complete, False otherwise
        """
        if next_conveyor_time:
            query = """
                SELECT process_status, process_complete_status
                FROM tb_combined_data
                WHERE conveyor_timestamp = %s
                AND equipment_timestamp <= %s
                ORDER BY equipment_timestamp DESC
            """
            params = (conveyor_time, next_conveyor_time)
        else:
            query = """
                SELECT process_status, process_complete_status
                FROM tb_combined_data
                WHERE conveyor_timestamp = %s
                ORDER BY equipment_timestamp DESC
            """
            params = (conveyor_time,)

        try:
            rows = self.query_executor.execute_query('analytics', query, params) or []
        except Exception as e:
            self.logger.error(f"Error verifying complete cycle for {conveyor_time}: {e}")
            return False

        if not rows:
            return False

        for status, complete_status in rows:
            if "complete_phase_2" in status or "process_complete" in status:
                return True
            if complete_status and ("complete_phase_2" in complete_status or "process_complete" in complete_status):
                return True

        return False

    def _process_individual_conveyor_request(self, conveyor_request, all_requests, index):
        """
        Processes an individual conveyor request
        
        Args:
            conveyor_request: Conveyor request to process
            all_requests: List of all requests
            index: Index of the current request in the list
        """
        conveyor_time = self._convert_to_datetime(conveyor_request[0])

        # Verify previous cycle if not the first request
        if index > 0:
            prev_conveyor_time = self._convert_to_datetime(all_requests[index - 1][0])
            current_conveyor_time = self._convert_to_datetime(conveyor_request[0])
            
            if not self._verify_complete_cycle(prev_conveyor_time, current_conveyor_time):
                self.logger.warning(
                    f"Previous cycle ({prev_conveyor_time}) is not complete "
                    f"before {current_conveyor_time}. Marking as interrupted and continuing."
                )
                # Here you could register the cycle as incomplete for audit

        # Determine end time for data search
        end_time = self._calculate_end_time(all_requests, index, conveyor_time)
        next_conveyor_time = self._get_next_conveyor_time(all_requests, index)

        # Search for code and equipment data
        code_data = self._search_corresponding_code(conveyor_time, next_conveyor_time)
        if not code_data:
            # Use string representation of time for the set
            time_str = conveyor_time.strftime('%Y-%m-%d %H:%M:%S')
            if time_str not in self.requests_without_code:
                self.logger.warning(f"No code found for conveyor request: {conveyor_time}")
                self.logger.warning(f"Skipping conveyor request {conveyor_time} - code not found")
                self.requests_without_code.add(time_str)
            return
        else:
            # If we found the code, remove the request from the no-code set just in case it was there
            time_str = conveyor_time.strftime('%Y-%m-%d %H:%M:%S')
            if time_str in self.requests_without_code:
                self.requests_without_code.remove(time_str)

        equipment_data = self.equipment_handler.get_equipment_data_by_time_range(conveyor_time, end_time)

        # Log processing information
        self._log_processing_info(conveyor_time, code_data, equipment_data)

        # Save data if there are equipment records
        if equipment_data['v24_records']:
            saved = self._save_combined_data(conveyor_time, code_data, equipment_data)
            if not saved:
                self.logger.error(
                    f"Failed saving combined data for {conveyor_time}; "
                    "will not update last_processed_time for this conveyor"
                )
                return

        # Update state ONLY if saved successfully
        self.state_manager.update_last_processed_time(conveyor_time)

    def _calculate_end_time(self, all_requests, current_index, conveyor_time):
        """
        Calculates the end time for equipment data search
        
        Args:
            all_requests: List of all requests
            current_index: Index of the current request
            conveyor_time: Time of the current conveyor
            
        Returns:
            datetime: End time for the search
        """
        if current_index < len(all_requests) - 1:
            next_conveyor_time = self._convert_to_datetime(all_requests[current_index + 1][0])
            return next_conveyor_time
        else:
            return conveyor_time + timedelta(minutes=10)

    def _get_next_conveyor_time(self, all_requests, current_index):
        """
        Gets the time of the next conveyor request if it exists
        
        Args:
            all_requests: List of all requests
            current_index: Index of the current request
            
        Returns:
            datetime or None: Time of the next conveyor or None if it doesn't exist
        """
        if current_index < len(all_requests) - 1:
            return self._convert_to_datetime(all_requests[current_index + 1][0])
        return None

    def _search_corresponding_code(self, conveyor_time, next_conveyor_time=None):
        """
        Searches for the code corresponding to a conveyor request
        
        Args:
            conveyor_time: Time of the conveyor request
            next_conveyor_time: Time of the next request (optional)
            
        Returns:
            tuple or None: Found code data or None if not found
        """
        search_end_time = next_conveyor_time if next_conveyor_time else conveyor_time + timedelta(minutes=10)

        query = """
            SELECT scanner_timestamp, product_code, operator_id, work_order
            FROM tb_product_scanner
            WHERE scanner_timestamp > %s 
            AND scanner_timestamp <= %s
            ORDER BY scanner_timestamp ASC
            LIMIT 1
        """

        result = self.query_executor.execute_query(
            'scanner', query, (conveyor_time, search_end_time)
        )

        if result:
            self.logger.info(f"Code found for conveyor {conveyor_time}: {result[0][0]}")
            return result[0]
        else:
            # Warning is not shown here, it's handled in the calling method
            return None

    def _save_combined_data(self, conveyor_time, code_data, equipment_data):
        """
        Saves combined data to the database
        
        Args:
            conveyor_time: Time of the conveyor request
            code_data: Found code data
            equipment_data: Retrieved equipment data
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        combined_data = {
            'conveyor_time': conveyor_time,
            'code_data': code_data,
            'equipment_data': equipment_data
        }

        if self.equipment_handler.save_combined_data_centered_conveyor(combined_data):
            self.logger.info(f"Combined data saved for conveyor request: {conveyor_time}")
            return True
        return False

    def _log_processing_info(self, conveyor_time, code_data, equipment_data):
        """
        Logs information about the current processing
        
        Args:
            conveyor_time: Time of the conveyor request
            code_data: Found code data
            equipment_data: Retrieved equipment data
        """
        v24_count = len(equipment_data['v24_records'])
        self.logger.info(f"Conveyor request: {conveyor_time}")
        self.logger.info(f"Associated code: {code_data[0]} - {code_data[1]}")
        self.logger.info(f"Equipment records found: {v24_count}")

    def _convert_to_datetime(self, time_value):
        """
        Converts a time value to a datetime object
        
        Args:
            time_value: Time value to convert (str or datetime)
            
        Returns:
            datetime: Datetime object
        """
        if isinstance(time_value, str):
            try:
                return datetime.strptime(time_value, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return datetime.strptime(time_value, '%Y-%m-%d %H:%M:%S.%f')
        return time_value