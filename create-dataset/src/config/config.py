from config.config_helpers import get_value_from_env


class Config:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-sandbox-01")
    PROCESS_TIMEOUT = int(get_value_from_env("PROCESS_TIMEOUT", "3400"))
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    RESPONSE_TIME_ALERT_THRESHOLD = int(get_value_from_env("RESPONSE_TIME_ALERT_THRESHOLD", "1000"))
    DATABASE = get_value_from_env("FIRESTORE_DB_NAME", "ons-sds-sandbox-01")
    RETAIN_DATASET_FIRESTORE = get_value_from_env("RETAIN_DATASET_FIRESTORE", "False")
    DATASET_BUCKET_NAME = get_value_from_env("DATASET_BUCKET_NAME", "ons-sds-sandbox-01-dataset")
    AUTODELETE_DATASET_BUCKET_FILE = get_value_from_env("AUTODELETE_DATASET_BUCKET_FILE", "True")
    PUBLISH_DATASET_TOPIC_ID = get_value_from_env("PUBLISH_DATASET_TOPIC_ID", "ons-sds-sandbox-01-publish-dataset")
    PUBLISH_DATASET_ERROR_TOPIC_ID = get_value_from_env("PUBLISH_DATASET_ERROR_TOPIC_ID", "ons-sds-sandbox-01-publish-dataset-error")
    LOG_EXECUTION_ID = get_value_from_env("LOG_EXECUTION_ID", "False")
    LOG_LEVEL = get_value_from_env("LOG_LEVEL", "INFO")
    CONF = get_value_from_env("CONF", "sandbox")


config = Config()
