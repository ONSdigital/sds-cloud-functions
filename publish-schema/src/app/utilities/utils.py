from pathlib import Path

from app.config.logging_config import logging
from app.models.schema_publish_errors import FilepathError

logger = logging.getLogger(__name__)


def split_filename(path: str) -> str | None:
    """
    Splits a filename without extension from the path.

    Parameters:
        path (str): the path to the file.

    Returns:
        str: the filename.
    """
    try:
        return Path(path).stem
    except TypeError:
        raise FilepathError(path) from None
