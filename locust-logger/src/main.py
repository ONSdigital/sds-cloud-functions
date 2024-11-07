import functions_framework
from logging_config import logging
import io as io

from locust_logger import LocustLogger

logger = logging.getLogger(__name__)
locust_logger = LocustLogger(logger)

@functions_framework.cloud_event
def log_locust_results(cloud_event):
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
    if not locust_logger.is_results_file(filepath):
        return
    
    logger.info("New performance test results file found.")
    
    # load results from file
    locust_logger.load_results(filepath)

    logger.info("Result loaded. Evaluating results...")

    # Evaluate results and log anoamlies
    if locust_logger.check_and_log_anomalies():
        logger.info("Performance test results evaluation completed. Results are normal.")


