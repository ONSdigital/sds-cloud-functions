import os


def can_cast_to_bool(value: str) -> bool:
    """
    Checks if a string value can be cast to a bool value when made lowercase.

    Parameters:
    value: env value
    """
    return value.lower() in ["true", "false"]


def get_bool_value(value: str) -> bool:
    """
    Returns true if the lowercase string value is true, otherwise returns false.

    Parameters:
    value: env value
    """
    return value.lower() == "true"


def format_value(value: str) -> str | bool:
    """
    Formats the value to return a boolean if it casts, otherwise return a string.

    Parameters:
    value: env value
    """
    return get_bool_value(value) if can_cast_to_bool(value) else value


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
        return format_value(value)

    if default_value is not None:
        return default_value

    raise Exception(f"The environment variable {env_value} must be set to proceed")
