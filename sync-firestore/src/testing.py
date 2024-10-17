#import functions_framework
from logging_config import logging
from responder import Responder
import firestore_schema_repository
import pg_schema_repository

logger = logging.getLogger(__name__)


logger.info("Syncing Firestore to Postgres sds-metrics db...")

# Sync schemas
logger.info("Syncing schemas...")
schemas = firestore_schema_repository.get_schema_metadata_collection()

logger.info(f"Found {len(schemas)} schemas to sync.")

for schema in schemas:
    if pg_schema_repository.query_schema_metrics(schema["survey_id"], schema["sds_schema_version"]) == 0:
        pg_schema_repository.insert_schema_metrics(
            schema["survey_id"],
            schema["title"],
            schema["schema_version"],
            schema["sds_schema_version"],
            schema["sds_published_at"],
        )

logger.info("Syncing Firestore is successful.")
