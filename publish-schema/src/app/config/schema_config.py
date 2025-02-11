from app.config.config_helpers import get_value_from_env


class SchemaConfig:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-jb")
    PROCESS_TIMEOUT = int(get_value_from_env("PROCESS_TIMEOUT", "540"))
    SDS_URL = get_value_from_env("SDS_URL", "test_url")
    SECRET_ID = get_value_from_env("SECRET_ID", "oauth-client-id")
    GITHUB_SCHEMA_URL = get_value_from_env(
        "GITHUB_SCHEMA_URL",
        "https://raw.githubusercontent.com/ONSdigital/sds-prototype-schema/refs/heads/main/",
    )
    POST_SCHEMA_ENDPOINT = get_value_from_env(
        "POST_SCHEMA_URL", "/v1/schema?survey_id="
    )
    GET_SCHEMA_METADATA_ENDPOINT = get_value_from_env(
        "GET_SCHEMA_METADATA_URL", "/v1/schema_metadata?survey_id="
    )
    PUBLISH_SCHEMA_ERROR_TOPIC_ID = get_value_from_env(
        "PUBLISH_SCHEMA_ERROR_TOPIC_ID", "publish-schema-error"
    )
    PUBLISH_SCHEMA_SUCCESS_TOPIC_ID = get_value_from_env(
        "PUBLISH_SCHEMA_SUCCESS_TOPIC_ID", "publish-schema-success"
    )
    PUBLISH_SCHEMA_QUEUE_TOPIC_ID = get_value_from_env(
        "PUBLISH_SCHEMA_QUEUE_TOPIC_ID", "publish-schema-queue"
    )
    FIRESTORE_DB_NAME = get_value_from_env(
        "FIRESTORE_DB_NAME", "ons-sds-jb-sds"
    )
    SCHEMA_BUCKET_NAME = get_value_from_env(
        "SCHEMA_BUCKET_NAME", "ons-sds-jb-sds-europe-west2-schema"
    )


CONFIG = SchemaConfig()
