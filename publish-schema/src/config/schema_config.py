import os


class ConfigHelpers:
    @staticmethod
    def can_cast_to_bool(value: str) -> bool:
        """
        Checks if a string value can be cast to a bool value when made lowercase.

        Parameters:
        value: env value
        """
        return value.lower() in ["true", "false"]

    @staticmethod
    def get_bool_value(value: str) -> bool:
        """
        Returns true if the lowercase string value is true, otherwise returns false.

        Parameters:
        value: env value
        """
        return value.lower() == "true"

    @staticmethod
    def format_value(value: str) -> str | bool:
        """
        Formats the value to return a boolean if it casts, otherwise return a string.

        Parameters:
        value: env value
        """
        return ConfigHelpers.get_bool_value(value) if ConfigHelpers.can_cast_to_bool(value) else value

    @staticmethod
    def get_value_from_env(env_value: str, default_value: str | None = None) -> str | bool:
        """
        Method to determine if a desired enviroment variable has been set and return it.
        If an enviroment variable or default value are not set an expection is raised.

        Parameters:
            env_value: value to check environment for
            default_value: optional argument to allow defaulting of values

        Returns:
            str: the environment value corresponding to the input
        """
        value = os.environ.get(env_value)

        if value is not None:
            return ConfigHelpers.format_value(value)

        if default_value is not None:
            return default_value

        raise Exception(f"The environment variable {env_value} must be set to proceed")


class SchemaConfig:
    PROJECT_ID = ConfigHelpers.get_value_from_env("PROJECT_ID", "ons-sds-jb")
    PROCESS_TIMEOUT = int(ConfigHelpers.get_value_from_env("PROCESS_TIMEOUT", "540"))
    SDS_URL = ConfigHelpers.get_value_from_env("SDS_URL", "test_url")
    SECRET_ID = ConfigHelpers.get_value_from_env("SECRET_ID", "oauth-client-id")
    GITHUB_SCHEMA_URL = ConfigHelpers.get_value_from_env(
        "GITHUB_SCHEMA_URL",
        "https://raw.githubusercontent.com/ONSdigital/sds-prototype-schema/refs/heads/main/",
    )
    POST_SCHEMA_ENDPOINT = ConfigHelpers.get_value_from_env(
        "POST_SCHEMA_URL", "/v1/schema?survey_id="
    )
    GET_SCHEMA_METADATA_ENDPOINT = ConfigHelpers.get_value_from_env(
        "GET_SCHEMA_METADATA_URL", "/v1/schema_metadata?survey_id="
    )
    PUBLISH_SCHEMA_ERROR_TOPIC_ID = ConfigHelpers.get_value_from_env(
        "PUBLISH_SCHEMA_ERROR_TOPIC_ID", "publish-schema-error"
    )
    PUBLISH_SCHEMA_SUCCESS_TOPIC_ID = ConfigHelpers.get_value_from_env(
        "PUBLISH_SCHEMA_SUCCESS_TOPIC_ID", "publish-schema-success"
    )
    PUBLISH_SCHEMA_QUEUE_TOPIC_ID = ConfigHelpers.get_value_from_env(
        "PUBLISH_SCHEMA_QUEUE_TOPIC_ID", "publish-schema-queue"
    )
    FIRESTORE_DB_NAME = ConfigHelpers.get_value_from_env(
        "FIRESTORE_DB_NAME", "ons-sds-jb-sds"
    )
    SCHEMA_BUCKET_NAME = ConfigHelpers.get_value_from_env(
        "SCHEMA_BUCKET_NAME", "ons-sds-jb-sds-europe-west2-schema"
    )


CONFIG = SchemaConfig()
