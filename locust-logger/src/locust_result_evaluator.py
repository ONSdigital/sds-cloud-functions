import pandas as pd

class LocustResultEvaluator:
    """
    Class to evaluate the results from the locust tests files.
    """
    @staticmethod
    def get_row_aggregated(df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to get the row of the aggregated results from the locust results.
        """
        return df.loc[df["Name"] == "Aggregated"]
    
    @staticmethod
    def get_column_average_response_time(df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to obtain the column average response time from the locust results.
        """
        return df["Average Response Time"]
    
    @staticmethod
    def get_column_failure_count(df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to obtain the column failure count from the locust results.
        """
        return df["Failure Count"]
    
    @staticmethod
    def get_value(df: pd.DataFrame) -> float:
        """
        Function to obtain the value from the locust results.
        """
        return df.values[0]
    
    @staticmethod
    def is_above_threshold(value, threshold) -> bool:
        """
        Function to check if the value is above the threshold.
        """
        return value > threshold