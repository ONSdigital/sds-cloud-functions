from config_helpers import get_value_from_env


class Config:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-sandbox-01")
    DATABASE = get_value_from_env("DATABASE", "ons-sds-sandbox-01-sds")
    PROCESS_TIMEOUT = int(get_value_from_env("PROCESS_TIMEOUT", "3400"))
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

config = Config()
