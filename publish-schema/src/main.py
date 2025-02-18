import base64

import functions_framework
from cloudevents.http import CloudEvent
from config.logging_config import logging
from config.schema_config import CONFIG
from models.schema_publish_errors import SchemaPublishError
from schema.schema import Schema
from services.pub_sub_service import PUB_SUB_SERVICE
from services.request_service import REQUEST_SERVICE
from services.schema_validator_service import SCHEMA_VALIDATOR_SERVICE

logger = logging.getLogger(__name__)


@functions_framework.cloud_event
def publish_schema(cloud_event: CloudEvent) -> None:
    """
    Retrieve, verify, and publish a schema to SDS.

    Parameters:
        cloud_event (CloudEvent): the CloudEvent containing the Pub/Sub message.
    """
    filepath = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")

    try:
        schema_json = REQUEST_SERVICE.fetch_raw_schema(filepath)

        schema = Schema.set_schema(schema_json, filepath)

        SCHEMA_VALIDATOR_SERVICE.validate_schema(schema)

        REQUEST_SERVICE.post_schema(schema)
    except SchemaPublishError as e:
        logger.error(e.error_message)
        PUB_SUB_SERVICE.send_message(e, CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID)
