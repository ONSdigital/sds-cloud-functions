import logging

from pubsub.pub_sub_publisher import PUB_SUB_PUBLISHER

logger = logging.getLogger(__name__)


class PubSubMessage:
    def __init__(
        self, message_type: str, message: str, schema_file: str, topic: str
    ) -> None:
        self.message_type = message_type
        self.message = message
        self.schema_file = schema_file
        self.topic = topic

        PUB_SUB_PUBLISHER.send_message(self)

    def json_message(self) -> dict:
        return {
            "message_type": self.message_type,
            "message": self.message,
            "schema_file": self.schema_file,
        }
