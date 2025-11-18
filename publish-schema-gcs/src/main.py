import functions_framework
from cloudevents.http import CloudEvent
from sds_common.config.logging_config import logging
from sds_common.enums.buckets import Bucket
from sds_common.repositories.bucket_loader import BucketLoader
from sds_common.schema.schema import Schema
from sds_common.services.bucket_service import BucketService
from sds_common.services.sds_schema_request_service import SDS_SCHEMA_REQUEST_SERVICE

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

    if not bucket_name == Bucket.SCHEMA_PUBLISH_BUCKET.value:
        logger.info(
            f"Bucket name is: {bucket_name} not schema publish bucket: {Bucket.SCHEMA_PUBLISH_BUCKET}, exiting function.")
        return None

    bucket_service = BucketService(Bucket.SCHEMA_PUBLISH_BUCKET, BucketLoader())
    schema_json = bucket_service.retrieve_json_file_from_bucket(file_name)
    schema = Schema.set_schema(schema_json)
    response = SDS_SCHEMA_REQUEST_SERVICE.post_schema(schema)

    if response.status_code == 200:
        logger.debug(f"Schema {file_name} published successfully, deleting schema file from bucket.")
        bucket_service.delete_file_from_bucket(file_name)

    return None
