import json

from config.config import CONFIG
from config.logging_config import logging
from services.pub_sub_service import PUB_SUB_SERVICE

logger = logging.getLogger(__name__)


class Error:
    """
    Base class for all error types - raises a RuntimeError to aid compatibility with GCP monitoring.
    """

    def __init__(self, error_type: str, message: str, filepath: str):
        self.error_type = error_type
        self.message = message
        self.filepath = filepath

    def generate_message(self) -> str:
        """Generate a JSON string representation of the PubSubMessage to be sent.

        Returns:
            str: JSON string representation of the PubSubMessage without topic.
        """
        return json.dumps(
            {
                "message_type": self.error_type,
                "message": self.message,
                "schema_file": self.filepath,
            }
        )

    def raise_error(self) -> None:
        """Log the error, send a Pub/Sub message, and raise a RuntimeError."""
        logger.error(
            f"Error Type: {self.error_type}, Message: {self.message}, Filepath: {self.filepath}"
        )
        PUB_SUB_SERVICE.send_message(self, CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID)
        raise RuntimeError(self) from self


class FilepathError(Error):
    def __init__(self, filepath: str):
        self.error_type = "FilepathError"
        self.message = "Failed to split filename from path."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaDuplicationError(Error):
    def __init__(self, filepath: str):
        self.error_type = "SchemaVersionError"
        self.message = "Schema version already exists in SDS for new survey."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaVersionMismatchError(Error):
    def __init__(self, filepath: str):
        self.error_type = "SchemaVersionError"
        self.message = "Schema version does not match filename."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SurveyIDError(Error):
    def __init__(self, filepath: str):
        self.error_type = "SurveyIdError"
        self.message = "Failed to fetch survey_id from schema JSON. Check the schema JSON contains a survey ID."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaVersionError(Error):
    def __init__(self, filepath: str):
        self.error_type = "SchemaVersionError"
        self.message = "Failed to fetch schema_version from schema JSON. Check the schema JSON contains a schema version."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class JSONDecodeError(Error):
    def __init__(self, filepath: str):
        self.error_type = "JSONDecodeError"
        self.message = "Failed to decode JSON response."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaFetchError(Error):
    def __init__(self, filepath: str, status_code: int, url: str):
        self.error_type = "SchemaFetchError"
        self.message = f"Failed to fetch schema from GitHub. Status code: {status_code}. URL: {url}"
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaPostError(Error):
    def __init__(self, filepath: str):
        self.error_type = "SchemaPostError"
        self.message = "Failed to post schema."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaMetadataError(Error):
    def __init__(self, survey_id: str):
        self.error_type = "SchemaMetadataError"
        self.message = "Failed to fetch schema metadata."
        self.filepath = "N/A"
        super().__init__(self.error_type, self.message, self.filepath)


class SecretError(Error):
    def __init__(self, filepath: str):
        self.error_type = "SecretError"
        self.message = (
            "Failed to access secret version from Google Cloud Secret Manager."
        )
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SecretKeyError(Error):
    def __init__(self, filepath: str):
        self.error_type = "SecretKeyError"
        self.message = "OAuth client ID not found in secret."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)
