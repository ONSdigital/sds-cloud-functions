from config_helpers import get_value_from_env


class Config:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-jb")
    PROCESS_TIMEOUT = int(get_value_from_env("PROCESS_TIMEOUT", "540"))
    SDS_URL = get_value_from_env("SDS_URL", "test_url")
    SECRET_ID = get_value_from_env("SECRET_ID", "oauth-client-id")
    GITHUB_SCHEMA_URL = get_value_from_env("GITHUB_SCHEMA_URL", "https://raw.githubusercontent.com/ONSdigital/sds-prototype-schema/refs/heads/main/")
    POST_SCHEMA_ENDPOINT = get_value_from_env("POST_SCHEMA_URL", "/v1/schema?survey_id=")
    GET_SCHEMA_METADATA_ENDPOINT = get_value_from_env("GET_SCHEMA_METADATA_URL", "/v1/schema_metadata?survey_id=")

config = Config()