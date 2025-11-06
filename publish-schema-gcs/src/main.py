import functions_framework
from cloudevents.http import CloudEvent
from sds_common.repositories.bucket_loader import bucket_loader
from sds_common.repositories.bucket_repository import BucketRepository
from sds_common.enums.buckets import Bucket

@functions_framework.cloud_event
def publish_schema_gcs(cloud_event: CloudEvent) -> None:
    """
    Retrieve, verify, and publish a schema to SDS; triggered by a CloudEvent from GCS when a schema file is uploaded.

    Parameters:
        cloud_event: The CloudEvent triggering the function.
    """
    bucket_name = cloud_event.data["bucket"]
    if bucket_name == Bucket.SCHEMA_PUBLISH_BUCKET.value:
        bucket_repository = BucketRepository(bucket_loader.fetch_bucket(Bucket.SCHEMA_PUBLISH_BUCKET.name))
        file_name = cloud_event.data["name"]
        schema_json = bucket_repository.get_file_as_json(file_name)
        return schema_json

    return None
