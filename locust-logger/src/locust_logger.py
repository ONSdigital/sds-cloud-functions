import pandas as pd
import io
from locust_result_evaluator import LocustResultEvaluator
from logging_config import logging
from anomaly_logs import anomaly_logs
from google.cloud import storage

from config import config

class LocustLogger:
    """
    Class to evaluate the results and log anomalies found from a performance test result file.
    """
    locust_result_file: str
    locust_result_df: pd.DataFrame

    def __init__(self, logger: logging):
        self.logger = logger
        self.locust_result_file = config.LOCUST_RESULT_FILENAME
        self.check_anomalies = [
            self._check_and_log_anomaly_failure_count,
            self._check_and_log_anomaly_avg_response_time
        ]

    def is_results_file(self, filepath) -> bool:
        """
        Function to read filepath of the file triggering the function
        
        Parameters:
        filepath: The path of the file in the bucket

        Returns:
        bool: True if the file is the results file, False otherwise
        """
        return self.locust_result_file in filepath
    
    def load_results(self, filepath: str) -> None:
        """
        Function to get the content of the results file from the bucket and convert it to a pandas DataFrame.
    
        Parameters:
        filepath: The path of the file in the bucket

        Raises:
        RuntimeError: If the file does not exist in the bucket
        """
        bucket_name = config.LOCUST_RESULT_BUCKET

        storage_client = storage.Client()
        blob = storage_client.bucket(bucket_name).blob(filepath)

        if not blob.exists():
            raise RuntimeError(f"Failed to read result file from bucket.")

        file = blob.download_as_string()

        self.locust_result_df = self._get_results_as_df(file)

    def check_and_log_anomalies(self) -> bool:
        """
        Function to check and log the anomalies found in the results.

        Returns:
        bool: True if no anomalies are found, False otherwise
        """
        has_anomalies = []
        for check_anomaly in self.check_anomalies:
            has_anomalies.append(check_anomaly())

        return all(has_anomalies)
    
    def _get_results_as_df(self, file: str) -> pd.DataFrame:
        """
        Function to convert the contents of a CSV file to a pandas DataFrame.
    
        Parameters:
        file: The contents of the CSV file as a string.
        """
        return pd.read_csv(io.StringIO(file.decode("utf-8")))
    
    # Helper functions to check for anomalies
    def _check_and_log_anomaly_failure_count(self) -> bool:
        """
        Function to log the failure count anomaly.

        Returns:
        bool: False if the failure count is above the threshold, True otherwise.
        """
        failure_count = self._extract_total_failure_count_from_results()

        if LocustResultEvaluator.is_above_threshold(failure_count, config.FAILURE_COUNT_ALERT_THRESHOLD):
            self.logger.error(
                f"{anomaly_logs.ANOMALY_LOG_FAILURE_COUNT}"
                f" Failure count is {failure_count} while the threshold is {config.FAILURE_COUNT_ALERT_THRESHOLD}."
            )
            return False
        
        self.logger.info("Failure count anomaly test passed.")
        return True
    
    def _check_and_log_anomaly_avg_response_time(self) -> bool:
        """
        Function to log the response time anomaly.

        Returns:
        bool: False if the average response time is above the threshold, True otherwise.
        """
        avg_response_time = self._extract_total_average_response_time_from_results()

        if LocustResultEvaluator.is_above_threshold(avg_response_time, config.RESPONSE_TIME_ALERT_THRESHOLD):
            self.logger.error(
                f"{anomaly_logs.ANOMALY_LOG_AVG_RESPONSE_TIME}"
                f" Average response time is {avg_response_time} ms while the threshold is {config.RESPONSE_TIME_ALERT_THRESHOLD} ms."
               )
            return False
        
        self.logger.info("Average response time anomaly test passed.")
        return True
    
    # Helper functions to extract information from the results
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
