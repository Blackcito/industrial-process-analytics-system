#operators.py
import logging
from datetime import date
from analytics.common_functions import format_date, calculate_stddev


class OperatorStatisticsCalculator:
    """
    Calculates statistics by operator based on tb_combined_data and dates.
    """
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.logger = logging.getLogger(__name__)

    def calculate_for_operator(self, operator: str, fi: date, ff: date) -> dict:
        start_date, end_date = format_date(fi), format_date(ff)
        cte = f"""
            WITH proc_op AS (
                SELECT
                    code_timestamp,
                    TIMESTAMPDIFF(MINUTE, MIN(timestamp_equipment), MAX(timestamp_equipment)) AS time_minutes,
                    MAX(CASE WHEN field_24 IN (59,63) THEN 1 ELSE 0 END) AS is_completed
                FROM tb_combined_data
                WHERE operator_code = %s
                  AND DATE(timestamp_conveyor) BETWEEN %s AND %s
                  group by code_timestamp
                
            )
        """
        query = cte + f"""
            SELECT
                COUNT(*) AS total_processes,
                AVG(time_minutes) AS average_time,
                MAX(time_minutes) AS max_time,
                MIN(time_minutes) AS min_time,
                {calculate_stddev('time_minutes')} AS standard_deviation,
                SUM(is_completed)/COUNT(*)*100 AS efficiency,
                COUNT(DISTINCT code_timestamp) AS different_products,
                COUNT(*)/(TIMESTAMPDIFF(HOUR, %s, %s)+1) AS processes_per_hour
            FROM proc_op
        """
        params = (operator, start_date, end_date, start_date, end_date)
        res = self.query_executor.execute_query('Combined', query, params, fetch_one=True)
        if not res:
            self.logger.info(f"No data for operator {operator} between {start_date}..{end_date}")
            return {}
        total, prom, maxi, mini, stdev, eff, different_products, per_hour = res
        return {
            'operator_code': operator,
            'start_date': start_date,
            'end_date': end_date,
            'total_processes': total or 0,
            'average_time_minutes': float(prom or 0),
            'max_time_minutes': float(maxi or 0),
            'min_time_minutes': float(mini or 0),
            'standard_deviation': float(stdev or 0),
            'average_efficiency': float(eff or 0),
            'different_products': different_products or 0,
            'processes_per_hour': float(per_hour or 0)
        }

class OperatorAnalyticsProcessor:
    """   
    Orquestrates the calculation and storage in tb_operator_statistics
    for each operator with activity on the indicated date.
    """
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.calculator = OperatorStatisticsCalculator(query_executor)
        self.logger = logging.getLogger(__name__)

    def run_for_operator(self, target_date: date) -> bool:
        """
        Ejecute the processing for all operators with data on `target_date`.
        """
        date_str = format_date(target_date)

        # 1) Obtener operadores únicos del día
        query_ops = """
            SELECT DISTINCT operator_code
            FROM tb_combined_data
            WHERE DATE(timestamp_conveyor) = %s
        """
        operators = self.query_executor.execute_query('Combined', query_ops, (date_str,)) or []
        if not operators:
            self.logger.info(f"No operators with data on {date_str}")
            return False

        success = True
        for (operator,) in operators:
            self.logger.info(f"Processing operator statistics for {operator} on {date_str}")

            # 2) Calcular estadísticas para ese operator en esa fecha
            stats = self.calculator.calculate_for_operator(operator, target_date, target_date)
            if not stats:
                self.logger.warning(f"No data for operator {operator} on {date_str}")
                continue

            # 3) Upsert de resultados
            upsert = f"""
                INSERT INTO tb_operator_statistics
                    (operator_code, start_date, end_date, total_processes,
                     average_time_minutes, max_time_minutes, min_time_minutes,
                     standard_deviation, average_efficiency, different_products,
                     processes_per_hour)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    total_processes=VALUES(total_processes),
                    average_time_minutes=VALUES(average_time_minutes),
                    max_time_minutes=VALUES(max_time_minutes),
                    min_time_minutes=VALUES(min_time_minutes),
                    standard_deviation=VALUES(standard_deviation),
                    average_efficiency=VALUES(average_efficiency),
                    different_products=VALUES(different_products),
                    processes_per_hour=VALUES(processes_per_hour),
                    updated_at=NOW()
            """
            params = (
                stats['operator_code'], stats['start_date'], stats['end_date'],
                stats['total_processes'], stats['average_time_minutes'], stats['max_time_minutes'],
                stats['min_time_minutes'], stats['standard_deviation'], stats['average_efficiency'],
                stats['different_products'], stats['processes_per_hour']
            )

            if not self.query_executor.execute_update('Combined', upsert, params):
                self.logger.error(f"Error saving stats for operator {operator} on {date_str}")
                success = False

        return success