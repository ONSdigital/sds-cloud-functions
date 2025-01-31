import json

from config.config import CONFIG
from google.cloud.pubsub_v1 import PublisherClient
from pubsub.pub_sub_message import PubSubMessage


class PubSubPublisher:
    def __init__(self):
        self.publisher = PublisherClient()

    def to_json(self) -> str:
        """
        Serialise the PubSubMessage to JSON string.

        Returns:
            str: The JSON string representation of the PubSubMessage.
        """
        return json.dumps(self.__dict__)

    def send_message(self, message: PubSubMessage) -> dict:
        """
        Sends a Pub/Sub message to a specified topic.

        Parameters:
            topic (str): The Pub/Sub topic to send the message to.

        Returns:
            dict: The message sent to the Pub/Sub topic.
        """
        topic_path = self.publisher.topic_path(CONFIG.PROJECT_ID, message.topic)
        self.publisher.publish(topic_path, data=message.json_message().encode("utf-8"))


PUB_SUB_PUBLISHER = PubSubPublisher()
