#main.py
import time
import redis
import os
from datetime import datetime, timedelta
from config import logging_config
from config.credentials import REDIS_CONFIG
from database import connection_manager, schema_manager, query_executor, state_manager
from processing import data_processor, equipment_data_handler
from analytics.daily import DailyAnalyticsProcessor
from analytics.operators import OperatorAnalyticsProcessor
from analytics.products import ProductAnalyticsProcessor
from analytics.processes import ProcessAnalyticsProcessor


class ProcessingSystem:
    """Main data processing system for continuous server execution"""
    
    def __init__(self):
        """Initializes all system components"""
        self.logger = logging_config.configure_logging()
        self.conn_manager = connection_manager.DatabaseConnectionManager()
        self.schema_mgr = schema_manager.SchemaManager(self.conn_manager)
        self.query_executor = query_executor.QueryExecutor(self.conn_manager)
        self.state_mgr = state_manager.StateManager(self.query_executor)
        self.equipment_handler = equipment_data_handler.EquipmentDataHandler(self.query_executor)
        self.data_proc = data_processor.DataProcessor(self.query_executor, self.state_mgr, self.equipment_handler)
        
        # Analytics processors
        self.daily_analytics = DailyAnalyticsProcessor(self.query_executor)
        self.operator_analytics = OperatorAnalyticsProcessor(self.query_executor)
        self.product_analytics = ProductAnalyticsProcessor(self.query_executor)
        self.process_analytics = ProcessAnalyticsProcessor(self.query_executor)
        
        self.cycle_count = 0
        self.is_running = False

        #Redis Config
        self.use_redis_flag = REDIS_CONFIG["USE_REDIS_FLAG"]
        if self.use_redis_flag:
            self.redis = redis.Redis(
                host=REDIS_CONFIG["REDIS_IP"],
                port=REDIS_CONFIG["REDIS_PORT"]
            )
            self.pubsub = self.redis.pubsub()
            self.pubsub.subscribe(REDIS_CONFIG["REDIS_CHANNEL"])


    def initialize_system(self):
        try:


            self.logger.info("=" * 60)
            self.logger.info("PROCESSING SYSTEM STARTED - Server Mode")
            self.logger.info("=" * 60)

            self.show_initial_statistics()
            self.initialize_system_state()
            self.logger.info(f"Last processed time: {self.state_mgr.last_processed_time}")
            self.logger.info("=" * 60)
            return True

        except Exception as e:
            self.logger.error(f"Error in system initialization: {e}")
            self.logger.exception("Initialization details:")
            return False

    def show_initial_statistics(self):
        """Shows initial system statistics"""
        try:
            stats = self.state_mgr.get_processing_statistics()
            self.logger.info("Initial system statistics:")
            self.logger.info(f"Total records: {stats.get('total_records', 0)}")
            self.logger.info(f"Unique conveyor requests: {stats.get('unique_conveyor_requests', 0)}")
            self.logger.info(f"Unique codes processed: {stats.get('unique_codes', 0)}")
            
            if stats.get('last_conveyor_time'):
                self.logger.info(f"Last conveyor request: {stats.get('last_conveyor_time')}")
                
        except Exception as e:
            self.logger.warning(f"Could not get initial statistics: {e}")

    def initialize_system_state(self):
        """Initializes system state from existing data if necessary"""
        if self.state_mgr.last_processed_time is None:
            self.logger.info("Initializing system from existing data...")
            self.state_mgr.initialize_from_existing_data()

    def execute_processing_cycle(self):
        """Executes a complete processing cycle"""
        try:
            self.cycle_count += 1
            start_time = datetime.now()

            self.logger.info(f"Starting cycle #{self.cycle_count}")
            
            # Process new conveyor requests
            self.process_conveyor_requests()
            
            # Execute analytics for current date
            self.execute_analytics()
            
            # Handle timing between cycles
            self.handle_cycle_timing(start_time)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in processing cycle #{self.cycle_count}: {e}")
            self.logger.exception("Cycle error details:")
            return False

    def process_conveyor_requests(self):
        """Processes conveyor requests from the current cycle"""
        try:
            new_conveyor_requests = self.data_proc.get_new_conveyor_requests()
            
            if new_conveyor_requests:
                self.logger.info(f"Processing {len(new_conveyor_requests)} conveyor requests")
                self.data_proc.process_new_conveyor_requests(new_conveyor_requests)
                
                # Display statistics every 10 cycles
                if self.cycle_count % 10 == 0:
                    self.show_cycle_statistics()
            else:
                self.logger.debug("No new conveyor requests to process")
                
        except Exception as e:
            self.logger.error(f"Error processing conveyor requests: {e}")
            raise

    def execute_analytics(self):
        """Executes all analytics processes for the current date"""
        try:
            today = datetime.now().date()
            self.logger.info(f"Executing analytics for {today}")
            
            # Run all analytics processes
            self.daily_analytics.run_for_date(today)
            self.operator_analytics.run_for_operator(today)
            self.process_analytics.run_for_processes(today)
            self.product_analytics.run_for_products(today)
            
        except Exception as e:
            self.logger.error(f"Error executing analytics: {e}")
            self.logger.exception("Analytics error details:")

    def show_cycle_statistics(self):
        """Shows statistics for the current cycle"""
        try:
            stats = self.state_mgr.get_processing_statistics()
            self.logger.info("Cycle statistics:")
            self.logger.info(f"Total records: {stats.get('total_records', 0)}")
            self.logger.info(f"Processed conveyor requests: {stats.get('unique_conveyor_requests', 0)}")
            
        except Exception as e:
            self.logger.warning(f"Error getting statistics: {e}")

    def handle_cycle_timing(self, start_time):
        """Handles the wait time between cycles depending on mode"""
        cycle_duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Cycle #{self.cycle_count} completed in {cycle_duration:.2f}s")

        if self.use_redis_flag:
            self.logger.info("Waiting for event in Redis (channel cerradora:1:scan)...")
            for msg in self.pubsub.listen():
                if msg["type"] == "message":
                    self.logger.info("Event received from Redis -> triggering next cycle")
                    break
        else:
            wait_time = max(5, 120 - cycle_duration)
            self.logger.info(f"Waiting {wait_time:.1f}s until next cycle")
            time.sleep(wait_time)

        self.logger.info("=" * 60)

    def run(self):
        """Main method that executes the system continuously"""
        self.is_running = True
        
        try:
            if not self.initialize_system():
                self.logger.error("System initialization failed")
                return False
                
            self.logger.info("System started successfully. Starting main cycle...")
            
            # Main execution cycle
            while self.is_running:
                if not self.execute_processing_cycle():
                    self.logger.warning("Cycle completed with errors, continuing...")
                    
        except KeyboardInterrupt:
            self.logger.info("User interrupt received")
            self.stop("SYSTEM STOPPED BY USER")
        except Exception as e:
            self.logger.error(f"Critical error in execution: {e}")
            self.logger.exception("Critical error details:")
            self.stop(f"CRITICAL ERROR: {str(e)}")
            
        return True

    def stop(self, message="System stopped"):
        """Stops the system in an orderly manner"""
        self.is_running = False
        self.logger.info("=" * 60)
        self.logger.info(message)
        self.logger.info("=" * 60)
        
        # Display final statistics
        self.show_final_statistics()
        
        # Close connections
        self.conn_manager.close_all()
        self.logger.info("Database connections closed")
        self.logger.info("SYSTEM STOPPED")

    def show_final_statistics(self):
        """Shows final system statistics"""
        try:
            stats = self.state_mgr.get_processing_statistics()
            self.logger.info("FINAL STATISTICS:")
            self.logger.info(f"Completed cycles: {self.cycle_count}")
            self.logger.info(f"Total records processed: {stats.get('total_records', 0)}")
            self.logger.info(f"Processed conveyor requests: {stats.get('unique_conveyor_requests', 0)}")
            self.logger.info(f"Last processed request: {stats.get('last_conveyor_time', 'N/A')}")
            
        except Exception as e:
            self.logger.warning(f"Could not get final statistics: {e}")

def main():
    """Main system entry function"""
    system = ProcessingSystem()
    system.run()

if __name__ == "__main__":
    main()