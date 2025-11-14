import functions_framework
from cloudevents.http import CloudEvent
from sds_common.repositories.bucket_loader import bucket_loader
from sds_common.repositories.bucket_repository import BucketRepository
from sds_common.enums.buckets import Bucket
from sds_common.schema.schema import Schema
from sds_common.services.sds_schema_request_service import SDS_SCHEMA_REQUEST_SERVICE
from sds_common.config.logging_config import logging

logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def publish_schema_gcs(cloud_event: CloudEvent) -> None:
    """
    Retrieve and publish a schema to SDS; triggered by a CloudEvent from GCS when a schema file is uploaded.
    Note: This function will not validate the schema before publishing. The function is only for testing.

    Parameters:
        cloud_event: The CloudEvent triggering the function.
    """
    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    if bucket_name == Bucket.SCHEMA_PUBLISH_BUCKET.value:
        bucket_repository = BucketRepository(bucket_loader.fetch_bucket(Bucket.SCHEMA_PUBLISH_BUCKET))
        logger.info("Fetching schema file: %s", file_name)
        schema_json = bucket_repository.get_file_as_json(file_name)
        schema = Schema.set_schema(schema_json)
        SDS_SCHEMA_REQUEST_SERVICE.post_schema(schema)
    else:
        logger.info(f"Bucket name is: {bucket_name} not schema publish bucket: {Bucket.SCHEMA_PUBLISH_BUCKET}, exiting function.")

    return None
