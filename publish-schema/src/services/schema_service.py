import logging

from pubsub.pub_sub_message import PubSubMessage
from request_service import REQUEST_SERVICE
from schema.schema import Schema
from config.config import CONFIG
from utilities.utils import split_filename

logger = logging.getLogger(__name__)


class SchemaService:
    def fetch_survey_id(self, schema: Schema) -> str:
        """
        Fetches the survey ID from the schema JSON.

        Parameters:
            schema (dict): the schema JSON.

        Returns:
            str: the survey ID.
        """
        try:
            survey_id = schema.get_json()["properties"]["survey_id"]["enum"][0]
        except (KeyError, IndexError) as e:
            message = PubSubMessage(
                "SurveyIdError", "Failed to fetch survey_id from schema JSON.", "N/A", CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID
            )
            raise RuntimeError(message.message) from e
        return survey_id

    def validate_schema(self, schema: Schema):
        """
        Validate the schema by verifying the version and checking for duplicate versions.

        Args:
            schema (Schema): _description_
        """
        self._verify_version(schema)
        self._check_duplicate_versions(schema)

    def _verify_version(self, schema: Schema):
        """
        Method to verify the schema version in the JSON matches the filename.

        Parameters:
            filename (str): the filename of the schema.
            schema (Schema): the schema object to be posted.
        """
        filepath = schema.get_filepath()
        trimmed_filename = split_filename(filepath)
        logger.info(f"Verifying schema version for {filepath}")
        try:
            schema_version = schema.get_json()["properties"]["schema_version"]["const"]
            if schema_version != trimmed_filename:
                message = PubSubMessage(
                    "SchemaVersionError",
                    f"Schema version for {filepath} does not match. Expected {trimmed_filename}, got {schema_version}",
                    filepath,
                    CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
                )
                raise RuntimeError(message.message)
        except KeyError as e:
            message = PubSubMessage(
                "KeyError",
                f"Schema version not found in {filepath}.",
                filepath,
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            raise RuntimeError(message.message) from e

    def _check_duplicate_versions(self, schema: Schema):
        """
        Method to call the schema_metadata endpoint and check that the schema_version for the new schema is not already present in SDS.

        Parameters:
            schema (Schema): the schema to be posted.
        """
        schema_metadata = REQUEST_SERVICE.get_schema_metadata(
            SCHEMA_SERVICE.get_survey_id()
        )

        # If the schema_metadata endpoint returns a 404, then the survey is new and there are no duplicate versions.
        if schema_metadata.status_code == 404:
            return

        new_schema_version = schema["properties"]["schema_version"]["const"]

        for version in schema_metadata.json():
            if new_schema_version == version["schema_version"]:
                message = PubSubMessage(
                    "SchemaVersionError",
                    f"Schema version {new_schema_version} already exists for survey {SCHEMA_SERVICE.get_survey_id()}",
                    "N/A",
                    CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
                )
                raise RuntimeError(message.message)


SCHEMA_SERVICE = SchemaService()
