from config.config import CONFIG
from pubsub.pub_sub_message import PubSubMessage
from pubsub.pub_sub_publisher import PUB_SUB_PUBLISHER


class Schema:
    def __init__(self, schema_json: dict, filepath: str) -> None:
        self.json = schema_json
        self.filepath = filepath
        self.survey_id = self.get_survey_id_from_json()
        self.schema_version = self.get_schema_version_from_json()

    def get_survey_id_from_json(self) -> str:
        """
        Fetches the survey ID from the schema JSON.

        Returns:
            str: the survey ID.
        """
        try:
            survey_id = self.json["properties"]["survey_id"]["enum"][0]
        except (KeyError, IndexError):
            message = PubSubMessage(
                "SurveyIdError",
                f"Failed to fetch survey_id from schema JSON. Check the schema JSON contains a survey ID. Filepath: "
                f"{self.filepath}",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            PUB_SUB_PUBLISHER.send_message(message)
            raise RuntimeError(message.message) from None
        return survey_id

    def get_schema_version_from_json(self):
        """
        Fetches the schema version from the schema JSON.

        Returns
            str: the schema version.
        """
        try:
            schema_version = self.json["properties"]["schema_version"]["const"]
        except KeyError:
            message = PubSubMessage(
                "KeyError",
                f"Failed to fetch schema_version from schema JSON. Check the schema JSON contains a schema version. "
                f"Filepath: {self.filepath}",
                self.filepath,
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            PUB_SUB_PUBLISHER.send_message(message)
            raise RuntimeError(message.message) from None
        return schema_version
