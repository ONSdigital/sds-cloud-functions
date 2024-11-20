import logging
import os


def get_log_level():
    """Get the logging level from the LOG_LEVEL environment variable, or use the default value of INFO.

    Returns: int: The logging level
    """
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    return getattr(logging, log_level)


logging.basicConfig(
    level=get_log_level(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
