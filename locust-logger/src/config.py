from config_helpers import get_value_from_env


class Config:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-performance")
    LOCUST_RESULT_BUCKET = get_value_from_env("LOCUST_RESULT_BUCKET", "ons-sds-performance-sds-locust-tasks-result")
    LOCUST_RESULT_FILENAME = get_value_from_env("LOCUST_RESULT_FILENAME", "result_stats.csv")
    RESPONSE_TIME_ALERT_THRESHOLD = int(get_value_from_env("MAX_RESPONSE_TIME", "100"))
    FAILURE_COUNT_ALERT_THRESHOLD = int(get_value_from_env("MAX_FAILURE_COUNT", "0"))
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

config = Config()