#processes.py
import logging
from datetime import date
from analytics.common_functions import format_date
from database.descriptions_dict import descriptions
class ProcessStatisticsCalculator:
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.logger = logging.getLogger(__name__)

    def get_description_code(self, code: str) -> str:
        return descriptions.get(code, "NO DESCRIPTION")

    def fetch_processes(self, target_date: date, cursor=None) -> list[dict]:
        date_str = format_date(target_date)

        query = """
            WITH procesos_secuencia AS (
                SELECT 
                    *,
                    LAG(timestamp_fin_proceso) OVER (ORDER BY timestamp_primer_cinta) AS timestamp_fin_proceso_anterior
                FROM (
                    SELECT
                        DATE(MIN(combined_data.timestamp_conveyor)) AS process_date,
                        combined_data.code_timestamp,
                        MIN(combined_data.product_code) AS product_code,
                        MIN(combined_data.operator_code) AS operator_code,
                        MIN(combined_data.ProductionOrder) AS ProductionOrder,
                        MIN(combined_data.timestamp_conveyor) AS timestamp_first_conveyor,
                        MIN(combined_data.code_timestamp) AS timestamp_first_code,
                        MIN(combined_data.timestamp_equipment) AS timestamp_first_plc,
                        MAX(combined_data.timestamp_equipment) AS timestamp_last_plc,

                        /* Process milestones */
                        MIN(CASE WHEN (combined_data.field_24 & 1) <> 0 THEN combined_data.timestamp_equipment END) AS timestamp_start_side_a,
                        MIN(CASE WHEN (combined_data.field_24 & 2) <> 0 THEN combined_data.timestamp_equipment END) AS timestamp_end_side_a,
                        MIN(CASE WHEN (combined_data.field_24 & 4) <> 0 THEN combined_data.timestamp_equipment END) AS timestamp_flip,
                        MIN(CASE WHEN (combined_data.field_24 & 8) <> 0 THEN combined_data.timestamp_equipment END) AS timestamp_start_side_b,
                        MIN(CASE WHEN (combined_data.field_24 & 16) <> 0 THEN combined_data.timestamp_equipment END) AS timestamp_end_side_b,
                        MIN(CASE WHEN (combined_data.field_24 & 32) <> 0 THEN combined_data.timestamp_equipment END) AS timestamp_end_process,

                        COUNT(*) AS records_count_v24
                    FROM tb_combined_data combined_data
                    WHERE DATE(combined_data.timestamp_conveyor) = %s
                    GROUP BY combined_data.code_timestamp
                ) processes_grouped
            )
            SELECT
                sequence_processes.process_date,
                TIME(sequence_processes.timestamp_first_plc) AS start_time,
                sequence_processes.product_code,
                sequence_processes.operator_code,
                sequence_processes.ProductionOrder,

                /* Total time (first plc until end process or last equipment) */
                GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_first_plc, 
                         COALESCE(sequence_processes.timestamp_end_process, sequence_processes.timestamp_last_plc))) / 60.0
                    AS total_time_minutes,

                /* Production time (start side A to end side B or last equipment) */
                CASE
                    WHEN sequence_processes.timestamp_start_side_a IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_start_side_a, 
                         COALESCE(sequence_processes.timestamp_end_side_b, sequence_processes.timestamp_last_plc))) / 60.0
                    ELSE 0
                END AS production_time_minutes,

                /* Incomplete process flag */
                CASE
                    WHEN sequence_processes.timestamp_end_process IS NULL THEN 1 ELSE 0
                END AS incomplete_process,

                sequence_processes.records_count_v24,

                /* Wait time */
                CASE
                    WHEN sequence_processes.timestamp_end_process_previous IS NOT NULL AND sequence_processes.timestamp_first_conveyor IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_end_process_previous, sequence_processes.timestamp_first_conveyor)) / 60.0
                    ELSE 0
                END AS wait_time_minutes,

                /* Conveyor to code time */
                CASE
                    WHEN sequence_processes.timestamp_first_conveyor IS NOT NULL AND sequence_processes.timestamp_first_code IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_first_conveyor, sequence_processes.timestamp_first_code)) / 60.0
                    ELSE 0
                END AS conveyor_code_time_minutes,

                CASE
                    WHEN sequence_processes.timestamp_first_conveyor IS NOT NULL AND sequence_processes.timestamp_first_plc IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_first_conveyor, sequence_processes.timestamp_first_plc)) / 60.0
                    ELSE 0
                END AS conveyor_plc_time_minutes,

                CASE
                    WHEN sequence_processes.timestamp_first_code IS NOT NULL AND sequence_processes.timestamp_first_plc IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_first_code, sequence_processes.timestamp_first_plc)) / 60.0
                    ELSE 0
                END AS code_start_time_minutes,

                /* Side A */
                CASE
                    WHEN sequence_processes.timestamp_start_side_a IS NOT NULL AND sequence_processes.timestamp_end_side_a IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_start_side_a, sequence_processes.timestamp_end_side_a)) / 60.0
                    ELSE 0
                END AS time_side_a_minutes,

                /* Automatic flip */
                CASE
                    WHEN sequence_processes.timestamp_flip IS NOT NULL AND sequence_processes.timestamp_start_side_b IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_end_side_a, sequence_processes.timestamp_start_side_b)) / 60.0
                    ELSE 0
                END AS auto_flip_time_minutes,

                /* Manual flip */
                CASE
                    WHEN sequence_processes.timestamp_flip IS NULL AND sequence_processes.timestamp_end_side_a IS NOT NULL AND sequence_processes.timestamp_start_side_b IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_end_side_a, sequence_processes.timestamp_start_side_b)) / 60.0
                    ELSE 0
                END AS manual_flip_time_minutes,

                /* Side B */
                CASE
                    WHEN sequence_processes.timestamp_start_side_b IS NOT NULL AND sequence_processes.timestamp_end_side_b IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_start_side_b, sequence_processes.timestamp_end_side_b)) / 60.0
                    ELSE 0
                END AS time_side_b_minutes,

                /* End time */
                CASE
                    WHEN sequence_processes.timestamp_end_side_b IS NOT NULL AND sequence_processes.timestamp_end_process IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_end_side_b, sequence_processes.timestamp_end_process)) / 60.0
                    ELSE 0
                END AS end_time_minutes,

                /* Pre-production: from first conveyor to first equipment (independent of code) */
                CASE
                    WHEN sequence_processes.timestamp_first_conveyor IS NOT NULL AND sequence_processes.timestamp_first_plc IS NOT NULL
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_first_conveyor, sequence_processes.timestamp_first_plc)) / 60.0
                    ELSE 0
                END AS pre_production_time_minutes,

                /* Post-production: from end of process or end of side B to last equipment */
                CASE
                    /* Priority 1: If there's end process and last equipment is later */
                    WHEN sequence_processes.timestamp_end_process IS NOT NULL AND sequence_processes.timestamp_last_plc IS NOT NULL 
                        AND sequence_processes.timestamp_last_plc > sequence_processes.timestamp_end_process
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_end_process, sequence_processes.timestamp_last_plc)) / 60.0
                    
                    /* Priority 2: If no end process but there's end side B and last equipment is later */
                    WHEN sequence_processes.timestamp_end_process IS NULL AND sequence_processes.timestamp_end_side_b IS NOT NULL 
                        AND sequence_processes.timestamp_last_plc IS NOT NULL AND sequence_processes.timestamp_last_plc > sequence_processes.timestamp_end_side_b
                    THEN GREATEST(0, TIMESTAMPDIFF(SECOND, sequence_processes.timestamp_end_side_b, sequence_processes.timestamp_last_plc)) / 60.0
                    
                    /* If there's no end process or end side B, don't calculate post-production */
                    ELSE 0
                END AS post_production_time_minutes

            FROM sequence_processes sequence_processes
            ORDER BY sequence_processes.timestamp_first_conveyor
        """

        processes = self.query_executor.execute_query('Combined', query, (date_str,), fetch_one=False) if cursor is None else cursor.execute(query, (date_str,)) or cursor.fetchall()
        if not processes:
            return []

        columns = [
            'process_date', 'start_time', 'product_code',
            'operator_code', 'production_order', 'total_time_minutes',
            'production_time_minutes', 'incomplete_process', 'records_count_v24',
            'wait_time_minutes', 'conveyor_code_time_minutes', 'conveyor_plc_time_minutes',
            'code_start_time_minutes', 'time_side_a_minutes', 'auto_flip_time_minutes',
            'manual_flip_time_minutes', 'time_side_b_minutes', 'end_time_minutes',
            'pre_production_time_minutes', 'post_production_time_minutes'
        ]
        
        results = []
        for row in processes:
            row_dict = dict(zip(columns, row))
            # Add description from Python dictionary
            row_dict["code_description"] = self.get_description_code(
                row_dict["product_code"]
            )
            results.append(row_dict)

        return results



class ProcessAnalyticsProcessor:
    def __init__(self, query_executor):
        self.query_executor = query_executor
        self.statistics_calculator = ProcessStatisticsCalculator(query_executor)
        self.logger = logging.getLogger(__name__)
        self.discarded_processes = set()  # Track processes already marked as discarded

    def run_for_processes(self, target_date: date) -> bool:
        insert_query = """
            INSERT INTO tb_process_statistics (
                timestamp_mark, process_date, start_time, product_code, code_description, 
                operator_code, production_order, total_time_minutes, production_time_minutes,
                incomplete_process, records_count_v24, wait_time_minutes,
                conveyor_code_time_minutes, conveyor_plc_time_minutes, code_start_time_minutes,
                time_side_a_minutes, auto_flip_time_minutes, manual_flip_time_minutes,
                time_side_b_minutes, end_time_minutes, pre_production_time_minutes,
                post_production_time_minutes, discarded_record
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                code_description = VALUES(code_description),
                total_time_minutes = VALUES(total_time_minutes),
                production_time_minutes = VALUES(production_time_minutes),
                incomplete_process = VALUES(incomplete_process),
                records_count_v24 = VALUES(records_count_v24),
                wait_time_minutes = VALUES(wait_time_minutes),
                conveyor_code_time_minutes = VALUES(conveyor_code_time_minutes),
                conveyor_plc_time_minutes = VALUES(conveyor_plc_time_minutes),
                code_start_time_minutes = VALUES(code_start_time_minutes),
                time_side_a_minutes = VALUES(time_side_a_minutes),
                auto_flip_time_minutes = VALUES(auto_flip_time_minutes),
                manual_flip_time_minutes = VALUES(manual_flip_time_minutes),
                time_side_b_minutes = VALUES(time_side_b_minutes),
                end_time_minutes = VALUES(end_time_minutes),
                pre_production_time_minutes = VALUES(pre_production_time_minutes),
                post_production_time_minutes = VALUES(post_production_time_minutes),
                discarded_record = VALUES(discarded_record),
                updated_at = NOW()
        """

        try:
            with self.query_executor.connection('Combined', close_after=False) as (connection, cursor):
                processes = self.statistics_calculator.fetch_processes(target_date, cursor=cursor)
                if not processes:
                    self.logger.info(f"No processes found for {target_date}")
                    return False

                params_list = []
                for process in processes:
                    # Determine if process should be marked as discarded
                    discarded_record = 1 if process["total_time_minutes"] > 30 else 0
                    
                    # Create unique identifier for this process
                    process_id = f"{process['product_code']}_{process['process_date']}"
                    
                    if discarded_record == 1:
                        # Only show warning if we haven't seen this process before
                        if process_id not in self.discarded_processes:
                            self.logger.warning(
                                f"Process {process['product_code']} marked as discarded (total_time > 30 min)"
                            )
                            self.discarded_processes.add(process_id)

                    timestamp_mark = f"{process['process_date']} {process['start_time']}"
                    params = (
                        timestamp_mark, process['process_date'], process['start_time'], process['product_code'], process['code_description'],
                        process['operator_code'], process['production_order'], process['total_time_minutes'],
                        process['production_time_minutes'], process['incomplete_process'], process['records_count_v24'],
                        process['wait_time_minutes'], process['conveyor_code_time_minutes'], process['conveyor_plc_time_minutes'],
                        process['code_start_time_minutes'], process['time_side_a_minutes'], process['auto_flip_time_minutes'],
                        process['manual_flip_time_minutes'], process['time_side_b_minutes'], process['end_time_minutes'],
                        process['pre_production_time_minutes'], process['post_production_time_minutes'],
                        discarded_record
                    )
                    params_list.append(params)

                if params_list:
                    cursor.executemany(insert_query, params_list)
                    connection.commit()

            return True
        except Exception as error:
            self.logger.error(f"Error in run_for_processes: {error}")
            return False