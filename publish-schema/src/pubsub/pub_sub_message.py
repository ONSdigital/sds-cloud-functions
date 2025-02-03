import json
from config.logging_config import logging

logger = logging.getLogger(__name__)


class PubSubMessage:
    def __init__(
        self, message_type: str, message: str, schema_file: str, topic: str
    ) -> None:
        self.message_type = message_type
        self.message = message
        self.schema_file = schema_file
        self.topic = topic

    def generate_message(self) -> str:
        """Generate a JSON string representation of the PubSubMessage to be sent.

        Returns:
            str: JSON string representation of the PubSubMessage without topic.
        """
        return json.dumps(
            {
                "message_type": self.message_type,
                "message": self.message,
                "schema_file": self.schema_file,
            }
        )
