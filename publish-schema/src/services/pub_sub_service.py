from config.config import CONFIG
from google.cloud.pubsub_v1 import PublisherClient
from models.error_models import Error


class PubSubService:
    def __init__(self):
        self.publisher = PublisherClient()

    def send_message(self, error: Error, topic_id: str) -> None:
        """
        Sends a Pub/Sub message to a specified topic.

        Parameters:
            error (Error): The PubSubMessage object to send.
            topic_id (str): The ID of the topic to send the message to.
        """
        topic_path = self.publisher.topic_path(CONFIG.PROJECT_ID, topic_id)
        message_json = error.generate_message()
        self.publisher.publish(topic_path, data=message_json.encode("utf-8"))
