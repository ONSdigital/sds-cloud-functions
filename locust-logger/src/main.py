import functions_framework
from google.cloud import storage
from logging_config import logging
from config import config
import pandas as pd
import io as io


@functions_framework.cloud_event
def handle_storage_event(cloud_event):
    """
    Cloud Function to respond to automated performance test results uploaded to a GCP bucket.
    Results uploaded are a directory with 4 files, one of which is the results_stats.csv.
    As 4 files are uploaded, this function is triggered 4 times but only the results summary file is processed.
    This function reads the contents of the results_stats.csv file and logs the results for the alerting system to pick up.

    Parameters:
    cloud_event: The event data from Eventarc.
    """

    # get the filepath of the file triggering the function
    data = cloud_event.data
    filepath = data["name"]

    # Check if the file triggering the function is the results file, if not, return.
    if check_filename(filepath):
        logging.info("New performance results file found.")
    else:
        return

    # Get the results from the file
    results = get_results(filepath)

    # Log the results
    write_logs(results)

    logging.info("Performance test results logged successfully.")

def check_filename(filepath: str) -> bool:
    """
    Function to read filepath of the file triggering the function and return True if it is the results file.
    
    Parameters:
    cloud_event: The event data from Eventarc.
    """

    return config.LOCUST_RESULT_FILENAME in filepath

def get_results(filepath: str)-> pd.DataFrame:
    """
    Function to get the contents of the results file from the bucket and convert it to a pandas DataFrame.

    Parameters:
    filepath: The path of the file in the bucket
    """

    bucket_name = config.LOCUST_RESULT_BUCKET

    storage_client = storage.Client()
    blob = storage_client.bucket(bucket_name).blob(filepath)

    if not blob.exists():
        raise Exception(f"File does not exist in the bucket {bucket_name}.")
    
    file = blob.download_as_string()
    return _get_results_as_df(file)

def _get_results_as_df(file: str) -> pd.DataFrame:
    """
    Function to convert the contents of a CSV file to a pandas DataFrame.

    Parameters:
    file: The contents of the CSV file as a string.
    """
    return pd.read_csv(io.StringIO(file.decode("utf-8")))

def write_logs(results_df: pd.DataFrame) -> None:
    """
    Function to write logs of any failed requests and average response times from the performance tests.
    
    Parameters:
    results_df: The DataFrame of the results file of the performance tests.
    """

    _log_failures(results_df)
    _log_response_times(results_df)

def _log_failures(results_df: pd.DataFrame) -> None:
    """
    Function to log the number of failed requests in the results of the performance tests.
    
    Parameters:
    results_df: The DataFrame of the results file f the performance tests.
    """
    # Go to the row of results where the 'Name' column is 'Aggregated' and get the 'Failure Count' value
    failures = results_df.loc[results_df["Name"] == "Aggregated"]["Failure Count"].values[0]
    if failures > 0:
        logging.error(f"Performance test failed with {failures} failed requests.")
    else:
        logging.info("Performance test passed with no failed requests.")

def _log_response_times(results_df: pd.DataFrame) -> None:
    """
    Function to log the average response time of the performance tests.
    
    Parameters:
    results_df: The DataFrame of the results file of the performance tests.
    """
    # Go to the row of results where the 'Name' column is 'Aggregated' and get the 'Average Response Time' value
    response_time = results_df.loc[results_df["Name"] == "Aggregated"]["Average Response Time"].values[0]
    if response_time > config.MAX_RESPONSE_TIME:
        logging.error(f"Average response time is too high: {response_time} ms")
    else:
        logging.info(f"Average response time is good: {response_time} ms")

