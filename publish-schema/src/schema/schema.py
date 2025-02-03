from config.config import CONFIG
from pubsub.pub_sub_message import PubSubMessage
from pubsub.pub_sub_publisher import PUB_SUB_PUBLISHER


class Schema:
    def __init__(self, schema_json: dict, filepath: str) -> None:
        self.json = schema_json
        self.filepath = filepath
        self.survey_id = self.set_survey_id()

    def set_survey_id(self) -> str:
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
                "Failed to fetch survey_id from schema JSON. Check the schema JSON for the correct format.",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            PUB_SUB_PUBLISHER.send_message(message)
            raise RuntimeError(message.message) from None
        return survey_id
