from config_helpers import get_value_from_env


class Config:
    PROJECT_ID = get_value_from_env("PROJECT_ID", "ons-sds-jb")
    LOCUST_RESULT_BUCKET = get_value_from_env("LOCUST_RESULT_BUCKET", "ons-sds-jb--sds-locust-tasks-result")
    LOCUST_RESULT_FILENAME = get_value_from_env("LOCUST_RESULT_FILENAME", "result_stats.csv")
    MAX_RESPONSE_TIME = int(get_value_from_env("MAX_RESPONSE_TIME", "100"))
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

config = Config()