import json

from config.config_factory import config
from google.cloud.pubsub_v1 import PublisherClient
from config.logging_config import logging
from models.dataset_models import DatasetError, DatasetMetadata

logger = logging.getLogger(__name__)


class PublisherService:
    def __init__(self):
        self.publisher = PublisherClient()

    def publish_data_to_topic(
        self,
        publish_data: DatasetMetadata | DatasetError,
        topic_id: str,
    ) -> None:
        """
        Publishes data to the pubsub topic.

        Parameters:
        publish_data: data to be sent to the pubsub topic,
        topic_id: unique identifier of the topic the data is published to
        """
        topic_path = self.publisher.topic_path(config.PROJECT_ID, topic_id)
        self._verify_topic_exists(topic_path)

        self.publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )

    def _verify_topic_exists(self, topic_path: str) -> None:
        """
        If the topic does not exist raises 500 global error.
        """
        try:
            self.publisher.get_topic(request={"topic": topic_path})
        except Exception as exc:
            raise RuntimeError("Topic not found") from exc


publisher_service = PublisherService()
