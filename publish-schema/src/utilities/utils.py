from config.logging_config import logging
from pathlib import Path

from config.config import CONFIG
from pubsub.pub_sub_message import PubSubMessage
from pubsub.pub_sub_publisher import PUB_SUB_PUBLISHER

logger = logging.getLogger(__name__)


def split_filename(path: str) -> str:
    """
    Splits the filename without extension from the path.

    Parameters:
        path (str): the path to the file.

    Returns:
        str: the filename.
    """
    try:
        return Path(path).stem
    except TypeError:
        raise_error(
            "Exception",
            "Failed to split filename from path.",
            path,
            CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
        )


def raise_error(message_type: str, message: str, schema_file: str, topic: str) -> None:
    """
    Raise a RuntimeError with the provided message and send a PubSubMessage.

    Parameters:
        message_type (str): the type of message.
        message (str): the error message.
        schema_file (str): the schema file.
        topic (str): the Pub/Sub topic.
    """
    message = PubSubMessage(
        message_type,
        message,
        schema_file,
        topic,
    )
    PUB_SUB_PUBLISHER.send_message(message)
    raise RuntimeError(message.message) from None
