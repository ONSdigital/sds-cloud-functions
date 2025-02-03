from config.config import CONFIG
from google.cloud.pubsub_v1 import PublisherClient
from pubsub.pub_sub_message import PubSubMessage


class PubSubPublisher:
    def __init__(self):
        self.publisher = PublisherClient()

    def send_message(self, message: PubSubMessage):
        """
        Sends a Pub/Sub message to a specified topic.

        Parameters:
            message (PubSubMessage): The PubSubMessage object to send.
        """
        topic_path = self.publisher.topic_path(CONFIG.PROJECT_ID, message.topic)
        message_json = message.generate_message()
        self.publisher.publish(topic_path, data=message_json.encode("utf-8"))


PUB_SUB_PUBLISHER = PubSubPublisher()
