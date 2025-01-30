import base64
import json
import logging

from config.config import CONFIG
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class PubSubMessage:
    def __init__(
        self, message_type: str, message: str, schema_file: str, topic: str
    ) -> None:
        self.message_type = message_type
        self.message = message
        self.schema_file = schema_file
        self.topic = topic

        self.send_message(self.topic)

    def to_json(self) -> str:
        """
        Serialise the PubSubMessage to JSON string.

        Returns:
            str: The JSON string representation of the PubSubMessage.
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
        topic_path = publisher.topic_path(CONFIG.PROJECT_ID, topic)
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
