import functions_framework
from logging_config import logging
from dataset_deleter import DatasetDeleter
from responder import Responder

logger = logging.getLogger(__name__)

@functions_framework.http
def delete_dataset(requests):
    dataset_deleter = DatasetDeleter()

    logger.info("Fetching dataset to delete...")

    dataset_deleter.fetch_dataset_deletion_from_collection()

    if dataset_deleter.guid is None:
        logger.info("No datasets to delete.")

        return Responder.send_response(
            "No datasets to delete.",
            "success",
            200,
        )

    logger.info(
        f"Dataset deletion request is found. Beginning process..."
    )
    logger.debug(f"GUID: {dataset_deleter.guid}, Marked ID: {dataset_deleter.marked_id}")

    doc_ref = dataset_deleter.fetch_dataset_with_guid()

    if not dataset_deleter.is_document_exists(doc_ref):
        # If the dataset does not exist, mark the deletion record as error
        logger.error(f"Error: Dataset is not found.")
        dataset_deleter.mark_dataset_as_error()

        return Responder.send_response(
            "Dataset is not found.",
            "error",
            404,
        )
    
    # If the dataset exists, delete the dataset and mark the deletion record as deleted
    logger.info("Deleting dataset...")
    dataset_deleter.mark_dataset_as_processing()

    if not dataset_deleter.delete_dataset_with_dataset_id(doc_ref):
        logger.info(
            f"Dataset deletion has reached the timeout. Process is suspended."
        )
        return Responder.send_response(
            "Dataset deletion has reached the timeout. Process is suspended.",
            "success",
            200,
        )

    # If the deletion process is successful (not timeout), mark the deletion record as deleted
    logger.info(f"Dataset deleted successfully.")

    # Mark the dataset as deleted with timestamp
    dataset_deleter.mark_dataset_as_deleted()

    return Responder.send_response(
        "Dataset deleted successfully.",
        "success",
        200,
    )
    
                