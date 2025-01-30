import logging

from config.config import CONFIG
from pubsub.pub_sub_message import PubSubMessage
from services.request_service import REQUEST_SERVICE
from utilities.utils import split_filename
from schema.schema import Schema

logger = logging.getLogger(__name__)


class SchemaValidatorService:
    def __init__(self, schema_json: dict, filepath: str) -> None:
        self.json = schema_json
        self.filepath = filepath
        self.survey_id = self.fetch_survey_id(schema_json)

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
            self.get_survey_id()
        )

        # If the schema_metadata endpoint returns a 404, then the survey is new and there are no duplicate versions.
        if schema_metadata.status_code == 404:
            return

        new_schema_version = schema["properties"]["schema_version"]["const"]

        for version in schema_metadata.json():
            if new_schema_version == version["schema_version"]:
                message = PubSubMessage(
                    "SchemaVersionError",
                    f"Schema version {new_schema_version} already exists for survey {self.get_survey_id()}",
                    "N/A",
                    CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
                )
                raise RuntimeError(message.message)

SCHEMA_SERVICE = SchemaValidatorService()