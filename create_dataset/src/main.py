import json

import functions_framework
from dataset.src.config.logging_config import logging
from services.dataset.dataset_bucket_service import DatasetBucketService
from services.dataset.dataset_processor_service import DatasetProcessorService

logger = logging.getLogger(__name__)


@functions_framework.http
def create_dataset(request):
    """
    Triggered by uploading a new create_dataset file to the
    create_dataset storage bucket. See the 'Cloud Functions' section
    in the README.md file for details as to how this function
    is set up.
    * The dataset_id is an auto generated GUID and the filename is saved as a new field in the metadata.
    """
    logger.info("Fetching new create_dataset...")
    filename = DatasetBucketService().try_fetch_oldest_filename_from_bucket()

    if not filename:
        logger.info("No create_dataset files found in bucket. Process is skipped")
        return json.dumps({"success": True}), 200, {"ContentType": "application/json"}

    logger.info("Uploading new create_dataset...")

    raw_dataset = DatasetBucketService().get_and_validate_dataset(filename)

    logger.info("Dataset obtained from bucket successfully.")
    logger.debug(f"Dataset: {raw_dataset}")

    DatasetProcessorService().process_raw_dataset(filename, raw_dataset)

    logger.info("Dataset uploaded successfully.")

    return json.dumps({"success": True}), 200, {"ContentType": "application/json"}