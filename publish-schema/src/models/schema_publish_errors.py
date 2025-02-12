import json

from config.logging_config import logging

logger = logging.getLogger(__name__)


class SchemaPublishError(Exception):
    def __init__(self, error_type: str, message: str, filepath: str):
        self.error_type = error_type
        self.message = message
        self.filepath = filepath
        self.error_message = f"Schema Publish Error - {self.error_type}: {self.message} Filepath: {self.filepath}"

    def generate_message_content(self) -> str:
        """
        Generates a JSON formatted string message from the error.

        Returns:
            str: The JSON formatted string message.
        """
        return json.dumps(
            {
                "error_type": self.error_type,
                "message": self.message,
                "filepath": self.filepath,
            }
        )


class FilepathError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "FilepathError"
        self.message = "Failed to split filename from path."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaDuplicationError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "SchemaDuplicationError"
        self.message = "Schema version already exists in SDS for new schema."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaVersionMismatchError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "SchemaVersionMismatchError"
        self.message = "Schema version does not match filename."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SurveyIDError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "SurveyIdError"
        self.message = "Failed to fetch survey_id from schema JSON. Check the schema JSON contains a survey ID."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaVersionError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "SchemaVersionError"
        self.message = "Failed to fetch schema_version from schema JSON. Check the schema JSON contains a schema version."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaJSONDecodeError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "SchemaJSONDecodeError"
        self.message = "Failed to decode the downloaded schema as JSON."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaFetchError(SchemaPublishError):
    def __init__(self, filepath: str, status_code: int, url: str):
        self.error_type = "SchemaFetchError"
        self.message = f"Failed to fetch schema from GitHub. Status code: {status_code}. URL: {url}"
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaPostError(
    SchemaPublishError,
):
    def __init__(self, filepath: str, status_code: int):
        self.error_type = "SchemaPostError"
        self.message = "Failed to post schema. Status code: {status_code}"
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SchemaMetadataError(SchemaPublishError):
    def __init__(self, survey_id: str, status_code: int):
        self.error_type = "SchemaMetadataError"
        self.message = f"Failed to fetch schema metadata for survey {survey_id}. Status code: {status_code}"
        self.filepath = "N/A"
        super().__init__(self.error_type, self.message, self.filepath)


class SecretAccessError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "SecretAccessError"
        self.message = (
            "Failed to access secret version from Google Cloud Secret Manager."
        )
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)


class SecretKeyError(SchemaPublishError):
    def __init__(self, filepath: str):
        self.error_type = "SecretKeyError"
        self.message = "OAuth client ID not found in secret."
        self.filepath = filepath
        super().__init__(self.error_type, self.message, filepath)
