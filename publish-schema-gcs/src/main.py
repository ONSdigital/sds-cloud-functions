import functions_framework
from cloudevents.http import CloudEvent
from sds_common.config.logging_config import logging
from sds_common.models.schema_publish_errors import SchemaPublishError
from sds_common.publishers.gcs_schema_publisher import GcsSchemaPublisher

logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def publish_schema_gcs(cloud_event: CloudEvent) -> None:
    """
    Retrieve and publish a schema to SDS; triggered by a CloudEvent from GCS when a schema file is uploaded.
    Note: This function will not validate the schema before publishing. The function is only for testing.

    Parameters:
        cloud_event: The CloudEvent triggering the function.
    """
    file_name = cloud_event.data["name"]

    try:
        publisher = GcsSchemaPublisher()
        response = publisher.publish_schema(file_name)
        if response.status_code == 200:
            logger.debug(f"Deleting schema file {file_name} from bucket.")
            publisher.cleanup(file_name)
    except SchemaPublishError as e:
        logger.error(e.error_message)

    return None
