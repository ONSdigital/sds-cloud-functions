from pubsub.pub_sub_message import PubSubMessage
from config.config import CONFIG

class Schema:
    def __init__(self, schema_json: dict, filepath: str) -> None:
        self.json = schema_json
        self.filepath = filepath
        self.survey_id = self.set_survey_id(schema_json)

    def get_json(self) -> dict:
        return self.json

    def get_filepath(self) -> str:
        return self.filepath

    def get_survey_id(self) -> str:
        return self.survey_id

    def set_survey_id(self) -> str:
        """
        Fetches the survey ID from the schema JSON.

        Parameters:
            schema (dict): the schema JSON.

        Returns:
            str: the survey ID.
        """
        try:
            survey_id = self.get_json()["properties"]["survey_id"]["enum"][0]
        except (KeyError, IndexError) as e:
            message = PubSubMessage(
                "SurveyIdError",
                "Failed to fetch survey_id from schema JSON.",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            raise RuntimeError(message.message) from e
        return survey_id