#products.py
import logging
from datetime import date
from analytics.common_functions import format_date, calculate_stddev

class ProductStatisticsCalculator:
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.logger = logging.getLogger(__name__)

    def calculate_for_product(self, product: str, fi: date, ff: date, cursor=None) -> dict:
        start_date, end_date = format_date(fi), format_date(ff)

        query = """
            SELECT
                code_description,
                COUNT(*) AS total_processes,
                AVG(total_time_minutes) AS average_total_time,
                AVG(production_time_minutes) AS average_production_time,
                MAX(total_time_minutes) AS max_total_time,
                MIN(total_time_minutes) AS min_total_time,
                STDDEV_POP(total_time_minutes) AS standard_deviation,
                SUM(CASE WHEN time_side_a_minutes > 0 AND time_side_b_minutes > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS efficiency,
                SUM(total_time_minutes) AS total_time_minutes
            FROM tb_process_statistics
            WHERE product_code = %s
            AND process_date BETWEEN %s AND %s
            GROUP BY code_description
        """

        params = (product, start_date, end_date)

        if cursor is None:
            res = self.query_executor.execute_query('Combined', query, params, fetch_one=True)
        else:
            cursor.execute(query, params)
            res = cursor.fetchone()

        if not res:
            self.logger.info(f"No processes for product {product} between {start_date} and {end_date}")
            return {}

        (desc, total, prom_total, prom_prod, maxi, mini, stdev, eff, total_time_minutes) = res

        active_hours = total_time_minutes / 60
        products_per_hour = total / active_hours if active_hours > 0 else 0


        return {
            'product_code': product,
            'code_description': desc or '',
            'start_date': start_date,
            'end_date': end_date,
            'total_processes': total or 0,
            'average_total_time_minutes': float(prom_total or 0),
            'average_production_time_minutes': float(prom_prod or 0),
            'max_total_time_minutes': float(maxi or 0),
            'min_total_time_minutes': float(mini or 0),
            'standard_deviation': float(stdev or 0),
            'average_efficiency': float(eff or 0),
            'products_per_hour': float(products_per_hour)
        }

class ProductAnalyticsProcessor:
    """
    Orquestrates the calculation and storage in tb_product_statistics.
    Opens a single connection per run.
    """
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.calculator = ProductStatisticsCalculator(query_executor)
        self.logger = logging.getLogger(__name__)

    def run_for_products(self, target_date: date) -> bool:
        date_str = format_date(target_date)

        query_codes = """
            SELECT DISTINCT product_code
            FROM tb_process_statistics
            WHERE process_date = %s
        """

        try:
            with self.query_executor.connection('Combined', close_after=False) as (conn, cursor):
                cursor.execute(query_codes, (date_str,))
                products = cursor.fetchall()
                if not products:
                    self.logger.info(f"No products for {date_str}")
                    return False

                params_list = []
                for (product,) in products:
                    self.logger.info(f"Processing statistics for product {product} on {date_str}")
                    stats = self.calculator.calculate_for_product(product, target_date, target_date, cursor=cursor)
                    if not stats:
                        self.logger.warning(f"No data for product {product} on {date_str}")
                        continue

                    params = (
                        stats['product_code'], stats['code_description'], stats['start_date'], stats['end_date'],
                        stats['total_processes'], stats['average_total_time_minutes'], stats['average_production_time_minutes'],
                        stats['max_total_time_minutes'], stats['min_total_time_minutes'], stats['standard_deviation'],
                        stats['average_efficiency'], stats['products_per_hour']
                    )
                    params_list.append(params)

                if params_list:
                    upsert = """
                        INSERT INTO tb_product_statistics (
                            product_code, code_description, start_date, end_date,
                            total_processes, average_total_time_minutes, average_production_time_minutes,
                            max_total_time_minutes, min_total_time_minutes, standard_deviation, average_efficiency,
                            products_per_hour
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            code_description=VALUES(code_description),
                            total_processes=VALUES(total_processes),
                            average_total_time_minutes=VALUES(average_total_time_minutes),
                            average_production_time_minutes=VALUES(average_production_time_minutes),
                            max_total_time_minutes=VALUES(max_total_time_minutes),
                            min_total_time_minutes=VALUES(min_total_time_minutes),
                            standard_deviation=VALUES(standard_deviation),
                            average_efficiency=VALUES(average_efficiency),
                            products_per_hour=VALUES(products_per_hour),
                            updated_at=NOW()
                    """
                    cursor.executemany(upsert, params_list)
                    conn.commit()

            return True
        except Exception as e:
            self.logger.error(f"Error in run_for_products: {e}")
            return False
