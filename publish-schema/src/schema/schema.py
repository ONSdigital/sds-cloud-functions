from __future__ import annotations

from models.schema_publish_errors import (
    SchemaVersionError,
    SurveyIDError,
)


class Schema:
    def __init__(
        self, schema_json: dict, filepath: str, survey_id: str, schema_version: str
    ) -> None:
        self.json = schema_json
        self.filepath = filepath
        self.survey_id = survey_id
        self.schema_version = schema_version

    @classmethod
    def set_schema(cls, schema_json: dict, filepath: str) -> Schema:
        """
        Sets the schema object with the survey ID and schema version from the schema JSON.
        Parameters:
            schema_json (dict): the schema JSON.
            filepath (str): the path to the schema JSON.
        Returns:
            Schema: the schema object.
        """
        try:
            survey_id = cls._get_survey_id_from_json(schema_json)
        except (KeyError, IndexError):
            raise SurveyIDError(filepath) from None

        try:
            schema_version = cls._get_schema_version_from_json(schema_json)
        except KeyError:
            raise SchemaVersionError(filepath) from None

        return cls(schema_json, filepath, survey_id, schema_version)

    @staticmethod
    def _get_survey_id_from_json(schema_json: dict) -> str | None:
        """
        Fetches the survey ID from the schema JSON.
        Returns:
            str: the survey ID.
        """
        return schema_json["properties"]["survey_id"]["enum"][0]

    @staticmethod
    def _get_schema_version_from_json(schema_json: dict) -> str | None:
        """
        Fetches the schema version from the schema JSON.
        Returns
            str: the schema version.
        """
        return schema_json["properties"]["schema_version"]["const"]
