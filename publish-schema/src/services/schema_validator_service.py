from config.config import CONFIG
from config.logging_config import logging
from pubsub.pub_sub_message import PubSubMessage
from pubsub.pub_sub_publisher import PUB_SUB_PUBLISHER
from schema.schema import Schema
from services.request_service import REQUEST_SERVICE
from utilities.utils import split_filename

logger = logging.getLogger(__name__)


class SchemaValidatorService:
    def validate_schema(self, schema: Schema):
        """
        Validate the schema by verifying the version and checking for duplicate versions.

        Args:
            schema (Schema): The schema object to validate.
        """
        self._verify_version(schema)
        self._check_duplicate_versions(schema)

    @staticmethod
    def _verify_version(schema: Schema):
        """
        Method to verify the schema version in the JSON matches the filename.

        Parameters:
            schema (Schema): the schema object to be posted.
        """
        logger.info(f"Verifying schema version for {schema.filepath}")
        trimmed_filename = split_filename(schema.filepath)
        if schema.schema_version != trimmed_filename:
            message = PubSubMessage(
                "SchemaVersionError",
                f"Schema version for {schema.filepath} does not match. Expected "
                f"{trimmed_filename} got {schema.schema_version}. Filepath: {schema.filepath}",
                schema.filepath,
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            PUB_SUB_PUBLISHER.send_message(message)
            raise RuntimeError(message.message)

    @staticmethod
    def _check_duplicate_versions(schema: Schema):
        """
        Check that the schema_version for the new schema is not already present in SDS.

        Parameters:
            schema (Schema): the schema to be posted.
        """
        schema_metadata = REQUEST_SERVICE.get_schema_metadata(schema.survey_id)

        # If the schema_metadata endpoint returns a 404, then the survey is new and there are no duplicate versions.
        if schema_metadata.status_code == 404:
            return

        for version in schema_metadata.json():
            if schema.schema_version == version["schema_version"]:
                message = PubSubMessage(
                    "SchemaVersionError",
                    f"Schema version {schema.schema_version} already exists for survey {schema.survey_id}. Schema "
                    f"file: {schema.filepath}",
                    "N/A",
                    CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
                )
                PUB_SUB_PUBLISHER.send_message(message)
                raise RuntimeError(message.message)


SCHEMA_VALIDATOR_SERVICE = SchemaValidatorService()
