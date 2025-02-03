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
        message = PubSubMessage(
            "Exception",
            "Failed to split filename from path.",
            path,
            CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
        )
        PUB_SUB_PUBLISHER.send_message(message)
        raise RuntimeError(message.message) from None
