import base64

import functions_framework
from cloudevents.http import CloudEvent
from sds_common.config.logging_config import logging
from sds_common.config.config import CONFIG
from sds_common.models.schema_publish_errors import SchemaPublishError
from sds_common.publishers.pubsub_schema_publisher import PubsubSchemaPublisher
from sds_common.services.pub_sub_service import PUB_SUB_SERVICE

logger = logging.getLogger(__name__)


@functions_framework.cloud_event
def publish_schema(cloud_event: CloudEvent) -> None:
    """
    Retrieve, verify, and publish a schema to SDS; triggered by Pub/Sub message containing schema filepath on GitHub.

    Parameters:
        cloud_event (CloudEvent): the CloudEvent containing the Pub/Sub message.
    """
    filepath = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")

    try:
        publisher = PubsubSchemaPublisher()
        publisher.publish(filepath)
    except SchemaPublishError as e:
        logger.error(e.error_message)
        PUB_SUB_SERVICE.send_message(e, CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID)
