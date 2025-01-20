from config_helpers import get_value_from_env


class Config:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-jb")
    PROCESS_TIMEOUT = int(get_value_from_env("PROCESS_TIMEOUT", "540"))
    SDS_URL = get_value_from_env("API_URL", "https://34.120.41.215.nip.io")
    SECRET_ID = get_value_from_env("SECRET_ID", "oauth-client-name")
    GITHUB_URL = get_value_from_env("GITHUB_URL", "https://raw.githubusercontent.com/ONSdigital/sds-prototype-schema/refs/heads/SDSS-823-schema-publication-automation-spike/")
    POST_SCHEMA_ENDPOINT = get_value_from_env("POST_SCHEMA_URL", "/v1/schema?survey_id=")
    GET_SCHEMA_METADATA_ENDPOINT = get_value_from_env("GET_SCHEMA_METADATA_URL", "/v1/schema_metadata?survey_id=")

config = Config()