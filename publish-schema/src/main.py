import base64

import functions_framework
from cloudevents.http import CloudEvent
from sds_common.config.logging_config import logging
from sds_common.config.schema_config import CONFIG
from sds_common.models.schema_publish_errors import SchemaPublishError
from sds_common.schema.schema import Schema
from sds_common.services.pub_sub_service import PUB_SUB_SERVICE
from sds_common.services.sds_schema_request_service import SDS_SCHEMA_REQUEST_SERVICE
from sds_common.services.schema_validator_service import SCHEMA_VALIDATOR_SERVICE
from sds_common.utilities.utils import fetch_raw_schema

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
        schema_json = fetch_raw_schema(filepath)

        schema = Schema.set_schema(schema_json, filepath)

        SCHEMA_VALIDATOR_SERVICE.validate_schema(schema)

        SDS_SCHEMA_REQUEST_SERVICE.post_schema(schema)
    except SchemaPublishError as e:
        logger.error(e.error_message)
        PUB_SUB_SERVICE.send_message(e, CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID)
