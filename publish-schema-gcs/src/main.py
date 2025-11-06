import functions_framework
from cloudevents.http import CloudEvent


@functions_framework.cloud_event
def publish_schema_gcs(cloud_event: CloudEvent) -> None:
    """
    Retrieve, verify, and publish a schema to SDS; triggered by a CloudEvent from GCS when a schema file is uploaded.

    Parameters:
        cloud_event: The CloudEvent triggering the function.
    """

    bucket_name = cloud_event.data["bucket"]
    file_name = cloud_event.data["name"]

    return None
