import functions_framework
from logging_config import logging
from responder import Responder
import firestore_schema_repository
import firestore_dataset_repository
import pg_schema_repository
import pg_dataset_repository

logger = logging.getLogger(__name__)

@functions_framework.http
def sync_firestore(request):
    logger.info("Syncing Firestore to Postgres sds-metrics db...")

    # Sync schemas
    logger.info("Syncing schemas...")
    schemas = firestore_schema_repository.get_schema_metadata_collection()

    logger.info(f"Found {len(schemas)} schemas to sync.")

    for schema in schemas:
        if pg_schema_repository.query_schema_metrics(schema["survey_id"], schema["sds_schema_version"]) == 0:
            logger.info(f"Inserting schema metrics for survey_id: {schema['survey_id']}")
            pg_schema_repository.insert_schema_metrics(
                schema["survey_id"],
                schema["title"],
                schema["schema_version"],
                schema["sds_schema_version"],
                schema["sds_published_at"],
            )

    logger.info("Sync schemas is successful.")

    # Sync dataset
    logger.info("Syncing datasets...")
    datasets = firestore_dataset_repository.get_dataset_metadata_collection()

    logger.info(f"Found {len(datasets)} datasets to sync.")

    for dataset in datasets:
        if pg_dataset_repository.query_dataset_metrics(dataset["survey_id"], dataset["sds_dataset_version"]) == 0:
            logger.info(f"Inserting dataset metrics for survey_id: {dataset['survey_id']}")
            pg_dataset_repository.insert_dataset_metrics(
                dataset["survey_id"],
                dataset["period_id"],
                dataset["sds_dataset_version"],
                dataset["sds_published_at"],
            )

    logger.info("Sync dataset is successful.")

    logger.info("Sync Firestore is successful.")

    return Responder.send_response(
            "Sync Firestore is successful.",
            "success",
            200,
        )