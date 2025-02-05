from config.logging_config import logging
from pathlib import Path

from config.config import CONFIG
from pubsub.pub_sub_message import PubSubMessage
from pubsub.pub_sub_publisher import PUB_SUB_PUBLISHER

logger = logging.getLogger(__name__)


def split_filename(path: str) -> str | None:
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
            path
        )


def raise_error(message_type: str, message: str, schema_file: str) -> None:
    """
    Raise a RuntimeError with the provided message and send a PubSubMessage.

    Parameters:
        message_type (str): the type of message.
        message (str): the error message.
        schema_file (str): the schema file.
    """
    message = PubSubMessage(
        message_type,
        message,
        schema_file,
        CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID
    )
    PUB_SUB_PUBLISHER.send_message(message)
    raise RuntimeError(message.message) from None
