import base64
import logging

import functions_framework
from cloudevents.http import CloudEvent
from request_service import REQUEST_SERVICE
from schema_service import SCHEMA_SERVICE

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
        schema = REQUEST_SERVICE.fetch_raw_schema(filepath)

        SCHEMA_SERVICE.set_survey_id(schema)

        SCHEMA_SERVICE.verify_version(filepath, schema)

        SCHEMA_SERVICE.check_duplicate_versions(schema)

        REQUEST_SERVICE.post_schema(schema, SCHEMA_SERVICE.get_survey_id(), filepath)
    except RuntimeError as e:
        logger.error(e)
        exit(1)
