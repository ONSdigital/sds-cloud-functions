import logging
from pathlib import Path

from pub_sub_error_message import PubSubErrorMessage

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
        message = PubSubErrorMessage(
            "Exception", "Failed to split filename from path.", path
        )
        logger.error(message.error_message)
