import functions_framework
from cloudevents.http import CloudEvent
from sds_common.config.logging_config import logging
from sds_common.publishers.gcs_schema_publisher import GcsSchemaPublisher
from sds_common.services.sds_schema_request_service import SdsSchemaRequestService

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

    publisher = GcsSchemaPublisher()
    response = publisher.publish(file_name)

    if response.status_code == 200:
        logger.debug(f"Deleting schema file {file_name} from bucket.")
        publisher.cleanup(file_name)

    return None
