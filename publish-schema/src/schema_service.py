import logging

from exception_message import ExceptionMessage
from pub_sub_error_message import PubSubErrorMessage
from request_service import REQUEST_SERVICE
from utils import split_filename

logger = logging.getLogger(__name__)


class SchemaService:
    def __init__(self):
        self.survey_id = None

    def fetch_survey_id(self, schema: dict) -> str:
        """
        Fetches the survey ID from the schema JSON.

        Parameters:
            schema (dict): the schema JSON.

        Returns:
            str: the survey ID.
        """
        try:
            survey_id = schema["properties"]["survey_id"]["enum"][0]
        except (KeyError, IndexError):
            message = PubSubErrorMessage(
                "SurveyIdError", "Failed to fetch survey_id from schema JSON.", "N/A"
            )
            logger.error(message.error_message)
            exit(1)
        return survey_id

    def verify_version(self, filepath: str, schema: dict) -> bool:
        """
        Method to verify the schema version in the JSON matches the filename.

        Parameters:
            filename (str): the filename of the schema.
            schema (dict): the schema to be posted.

        Returns:
            bool: True if the schema version matches the filename.
        """
        logger.info(f"Verifying schema version for {filepath}")
        filename = split_filename(filepath)
        try:
            if schema["properties"]["schema_version"]["const"] == filename:
                return True
            else:
                message = PubSubErrorMessage(
                    "SchemaVersionError",
                    f"Schema version for {filepath} does not match. Expected {filename}, got {schema['properties']['schema_version']['const']}",
                    filepath,
                )
                raise ExceptionMessage(message.error_message)
        except KeyError:
            message = PubSubErrorMessage(
                "SchemaVersionError",
                f"Schema version not found in {filepath}.",
                filepath,
            )
            logger.error(message.error_message)
            exit(1)

    def check_duplicate_versions(self, schema: dict) -> bool:
        """
        Method to call the schema_metadata endpoint and check that the schema_version for the new schema is not already present in SDS.

        Parameters:
            schema (dict): the schema to be posted.

        Returns:
            bool: True if there are no duplicate versions, False otherwise.
        """
        schema_metadata = REQUEST_SERVICE.get_schema_metadata(self.survey_id)

        # If the schema_metadata endpoint returns a 404, then the survey is new and there are no duplicate versions.
        if schema_metadata.status_code == 404:
            return True

        new_schema_version = schema["properties"]["schema_version"]["const"]

        for version in schema_metadata.json():
            if new_schema_version == version["schema_version"]:
                message = PubSubErrorMessage(
                    "SchemaVersionError",
                    f"Schema version {new_schema_version} already exists for survey {self.survey_id}",
                    "N/A",
                )
                raise ExceptionMessage(message.error_message)
        return True

    def get_survey_id(self):
        return self.survey_id

    def set_survey_id(self, schema: dict):
        self.survey_id = self.fetch_survey_id(schema)


SCHEMA_SERVICE = SchemaService()
