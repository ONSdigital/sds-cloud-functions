import json

from config.config import CONFIG
from google.cloud.pubsub_v1 import PublisherClient
from models.error_models import SchemaPublishError


class PubSubService:
    def __init__(self):
        self.publisher = PublisherClient()

    def send_message(self, error: SchemaPublishError, topic_id: str) -> None:
        """
        Sends a Pub/Sub message to the specified topic.

        Parameters:
            error (SchemaPublishError): The SchemaPublishError object containing message info to send.
            topic_id (str): The ID of the topic to send the message to.
        """
        topic_path = self.publisher.topic_path(CONFIG.PROJECT_ID, topic_id)
        message_json = self.generate_message(error)
        self.publisher.publish(topic_path, data=message_json.encode("utf-8"))

    @staticmethod
    def generate_message(error: SchemaPublishError) -> str:
        """
        Generates a JSON message to send to Pub/Sub from the relevant attributes of a schema object.

        Parameters:
            error (SchemaPublishError): The SchemaPublishError object to generate the message from.
        Returns:
            str: The JSON message to send to Pub/Sub.
        """
        return json.dumps(
            {
                "error_type": error.error_type,
                "message": error.message,
                "filepath": error.filepath,
            }
        )


PUB_SUB_SERVICE = PubSubService()
