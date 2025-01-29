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

    schema = REQUEST_SERVICE.fetch_raw_schema(filepath)

    if SCHEMA_SERVICE.verify_version(filepath, schema):
        logger.info(f"Schema version for {filepath} verified.")

    survey_id = SCHEMA_SERVICE.fetch_survey_id(schema)

    if SCHEMA_SERVICE.check_duplicate_versions(schema, survey_id):
        logger.info(f"Schema version for {filepath} is not a duplicate.")

    REQUEST_SERVICE.post_schema(schema, survey_id, filepath)
