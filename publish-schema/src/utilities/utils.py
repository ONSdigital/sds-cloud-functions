import logging
from pathlib import Path

from pubsub.pub_sub_message import PubSubMessage
from config.config import CONFIG

logger = logging.getLogger(__name__)


def split_filename(path: str) -> str:
    """
    Splits the filename without extension from the path.

    Parameters:
        path (str): the path to the schema JSON.

    Returns:
        str: the filename.
    """
    try:
        return Path(path).stem
    except Exception:
        message = PubSubMessage(
            "Exception", "Failed to split filename from path.", path, CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID
        )
        logger.error(message.message)
