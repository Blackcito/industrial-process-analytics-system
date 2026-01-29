#Daily.py
import logging
from datetime import date
from analytics.common_functions import format_date, calculate_stddev

class DailyStatisticsCalculator:
    """
    Calculates daily statistics (average, maximum, minimum, standard deviation,
    efficiency and wait times) based on data from tb_combined_data for MariaDB.
    """
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.logger = logging.getLogger(__name__)

    def calculate_for_date(self, target_date: date) -> dict:
        date_str = format_date(target_date)
        processes_cte = f"""
            WITH processes AS (
                SELECT
                    code_timestamp,
                    TIMESTAMPDIFF(MINUTE,
                        MIN(timestamp_equipment), MAX(timestamp_equipment)
                    ) AS time_minutes,
                    TIMESTAMPDIFF(SECOND,
                        MIN(timestamp_conveyor), MIN(CASE WHEN v24_description = 'start_side_a' THEN timestamp_equipment END)
                    ) AS wait_time_seconds,
                    MAX(CASE WHEN field_24 IN (59, 63) THEN 1 ELSE 0 END) AS is_completed
                FROM tb_combined_data
                WHERE DATE(timestamp_conveyor) = %s
                GROUP BY code_timestamp
            )
        """
        stats_query = processes_cte + f"""
            SELECT
                COUNT(*) AS total_processes,
                AVG(time_minutes) AS average_time,
                MAX(time_minutes) AS max_time,
                MIN(time_minutes) AS min_time,
                {calculate_stddev('time_minutes')} AS standard_deviation,
                SUM(is_completed)/COUNT(*)*100 AS efficiency,
                SUM(is_completed) AS completed_processes,
                COUNT(*)-SUM(is_completed) AS incomplete_processes,
                AVG(wait_time_seconds) AS average_wait_time_seconds,
                MAX(wait_time_seconds) AS max_wait_time_seconds,
                MIN(wait_time_seconds) AS min_wait_time_seconds
            FROM processes
        """
        result = self.query_executor.execute_query('Combined', stats_query, (date_str,), fetch_one=True)
        if not result:
            self.logger.info(f"No data for {date_str}")
            return {}
        (
            total, prom, maxi, mini, stdev, eff,
            comp, incom, esp_prom, esp_max, esp_min
        ) = result
        return {
            'date': date_str,
            'total_processes': total or 0,
            'average_time_minutes': float(prom or 0),
            'max_time_minutes': float(maxi or 0),
            'min_time_minutes': float(mini or 0),
            'standard_deviation': float(stdev or 0),
            'average_efficiency': float(eff or 0),
            'completed_processes': comp or 0,
            'incomplete_processes': incom or 0,
            'average_wait_time_seconds': float(esp_prom or 0),
            'max_wait_time_seconds': float(esp_max or 0),
            'min_wait_time_seconds': float(esp_min or 0)
        }

class DailyAnalyticsProcessor:
    """
    Orchestrates daily calculation and storage in tb_daily_statistics.
    """
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.calculator = DailyStatisticsCalculator(query_executor)
        self.logger = logging.getLogger(__name__)

    def run_for_date(self, target_date: date = None) -> bool: # type: ignore
        if target_date is None:
            target_date = date.today()
        stats = self.calculator.calculate_for_date(target_date)
        if not stats:
            return False
        upsert = """
            INSERT INTO tb_daily_statistics
                (date, total_processes, average_time_minutes, max_time_minutes,
                 min_time_minutes, standard_deviation, average_efficiency,
                 completed_processes, incomplete_processes,
                 average_wait_time_seconds, max_wait_time_seconds,
                 min_wait_time_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_processes=VALUES(total_processes),
                average_time_minutes=VALUES(average_time_minutes),
                max_time_minutes=VALUES(max_time_minutes),
                min_time_minutes=VALUES(min_time_minutes),
                standard_deviation=VALUES(standard_deviation),
                average_efficiency=VALUES(average_efficiency),
                completed_processes=VALUES(completed_processes),
                incomplete_processes=VALUES(incomplete_processes),
                average_wait_time_seconds=VALUES(average_wait_time_seconds),
                max_wait_time_seconds=VALUES(max_wait_time_seconds),
                min_wait_time_seconds=VALUES(min_wait_time_seconds),
                updated_at=NOW()
        """
        params = (
            stats['date'], stats['total_processes'], stats['average_time_minutes'],
            stats['max_time_minutes'], stats['min_time_minutes'],
            stats['standard_deviation'], stats['average_efficiency'],
            stats['completed_processes'], stats['incomplete_processes'],
            stats['average_wait_time_seconds'], stats['max_wait_time_seconds'],
            stats['min_wait_time_seconds']
        )
        success = self.query_executor.execute_update('Combined', upsert, params)
        if success:
            self.logger.info(f"Daily statistics saved for {stats['date']}")
        else:
            self.logger.error(f"Error saving statistics for {stats['date']}")
        return success
