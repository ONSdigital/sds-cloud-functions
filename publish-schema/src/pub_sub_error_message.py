import base64
import json
from google.cloud import pubsub_v1
from config import config
from logging_config import logging

logger = logging.getLogger(__name__)


class PubSubErrorMessage:
    def __init__(self, error_type: str, error_message: str, schema_file: str) -> None:
        self.error_type = error_type
        self.error_message = error_message
        self.schema_file = schema_file

        self.send_message(config.PUBLISH_SCHEMA_ERROR_TOPIC_ID)

    def to_json(self) -> str:
        """
        Serialise the PubSubErrorMessage to JSON string.

        Returns:
            str: The JSON string representation of the PubSubErrorMessage.
        """
        return json.dumps(self.__dict__)

    def send_message(self, topic: str) -> dict:
        """
        Sends a Pub/Sub message to a specified topic.

        Parameters:
            topic (str): The Pub/Sub topic to send the message to.

        Returns:
            dict: The message sent to the Pub/Sub topic.
        """
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(config.PROJECT_ID, topic)
        message = self.build_pubsub_message()
        future = publisher.publish(topic_path, message.encode("utf-8"))
        future.result()
        return message

    def build_pubsub_message(self) -> dict:
        """
        Build the Pub/Sub message to send to the error topic.

        Returns:
            dict: The Pub/Sub message to send to the error topic.
        """
        message = {
            "messages": [
                {
                    "data": base64.b64encode(self.to_json().encode("utf-8")).decode(
                        "utf-8"
                    )
                }
            ]
        }
        return message
