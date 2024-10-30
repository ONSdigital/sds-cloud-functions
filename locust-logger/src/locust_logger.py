import pandas as pd
from locust_result_evaluator import LocustResultEvaluator
from logging_config import logging
from anomaly_logs import anomaly_logs
from google.cloud import storage

from config import config

class LocustLogger:
    """
    """
    locust_result_file: str
    locust_result_df: pd.DataFrame

    def __init__(self):
        self.locust_result_file = config.LOCUST_RESULT_FILENAME

    def is_results_file(self, filepath):
        """
        Function to read filepath of the file triggering the function and return True if it is the results file.
        
        Parameters:
        filepath: The path of the file in the bucket
        """
        return self.locust_result_file in filepath
    
    def load_results(self, filepath: str):
        """
        Function to get the contents of the results file from the bucket and convert it to a pandas DataFrame.
    
        Parameters:
        filepath: The path of the file in the bucket
        """
        bucket_name = config.LOCUST_RESULT_BUCKET

        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(filepath)

        if not blob.exists():
            raise RuntimeError(f"Failed to read result file from bucket.")

        file = blob.download_as_string()

        self.locust_result_df = self._get_results_as_df(file)
    
    def log_anomaly_failure_count(self, logger: logging) -> bool:
        """
        Function to log the failure count anomaly.
        """
        failure_count = self._extract_total_failure_count_from_results()

        if LocustResultEvaluator.is_above_threshold(failure_count, config.FAILURE_COUNT_ALERT_THRESHOLD):
            logger.error(
                f"{anomaly_logs.ANOMALY_LOG_FAILURE_COUNT}"
                f" Failure count is {failure_count} while the threshold is {config.FAILURE_COUNT_ALERT_THRESHOLD}."
            )
            return False
        
        return True
    
    def log_anomaly_avg_response_time(self, logger: logging) -> bool:
        """
        Function to log the response time anomaly.
        """
        avg_response_time = self._extract_total_average_response_time_from_results()

        if LocustResultEvaluator.is_above_threshold(avg_response_time, config.RESPONSE_TIME_ALERT_THRESHOLD):
            logger.error(
                f"{anomaly_logs.ANOMALY_LOG_AVG_RESPONSE_TIME}"
                f" Average response time is {avg_response_time} ms while the threshold is {config.RESPONSE_TIME_ALERT_THRESHOLD} ms."
               )
            return False
        
        return True
    
    def _get_results_as_df(self, file: str) -> pd.DataFrame:
        """
        Function to convert the contents of a CSV file to a pandas DataFrame.
    
        Parameters:
        file: The contents of the CSV file as a string.
        """
        return pd.read_csv(file, encoding="utf-8")
    
    def _extract_total_failure_count_from_results(self) -> int:
        """
        Function to extract the total failure count from the results.
        """
        row_aggregated = LocustResultEvaluator.get_row_aggregated(self.locust_result_df)
        col_failure_count = LocustResultEvaluator.get_column_failure_count(row_aggregated)

        return LocustResultEvaluator.get_value(col_failure_count)
    
    def _extract_total_average_response_time_from_results(self) -> float:
        """
        Function to extract the total average response time from the results.
        """
        row_aggregated = LocustResultEvaluator.get_row_aggregated(self.locust_result_df)
        col_avg_response_time = LocustResultEvaluator.get_column_average_response_time(row_aggregated)

        return LocustResultEvaluator.get_value(col_avg_response_time)
