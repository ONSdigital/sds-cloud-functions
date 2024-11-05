from config_helpers import get_value_from_env


class Config:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-jb")
    PROCESS_TIMEOUT = int(get_value_from_env("PROCESS_TIMEOUT", "3400"))
    API_URL = get_value_from_env("API_URL", "https://34.120.41.215.nip.io")
    SECRET_ID = get_value_from_env("SECRET_ID", "oauth-client-name")

config = Config()