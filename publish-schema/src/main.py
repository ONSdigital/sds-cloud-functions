import base64
import json
import logging
from pathlib import Path

import functions_framework
import requests
from cloudevents.http import CloudEvent
from config import config
from http_manager import http_manager
from pub_sub_error_message import PubSubErrorMessage

logger = logging.getLogger(__name__)


@functions_framework.cloud_event
def publish_schema(cloud_event: CloudEvent) -> None:
    """
    Retrieve, verify, and publish a schema to SDS.

    Parameters:
        cloud_event (CloudEvent): the CloudEvent containing the  Pub/Sub message.
    """
    filepath = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")

    schema = fetch_raw_schema(filepath)

    if verify_version(filepath, schema):
        logger.info(f"Schema version for {filepath} verified.")

    survey_id = fetch_survey_id(schema)

    if check_duplicate_versions(schema, survey_id):
        logger.info(f"Schema version for {filepath} is not a duplicate.")

    post_schema(schema, survey_id, filepath)


def fetch_raw_schema(path: str) -> dict:
    """
    Fetches the schema from the ONSdigital GitHub repository.

    Parameters:
        path (str): the path to the schema JSON.

    Returns:
        dict: the schema JSON.
    """
    url = config.GITHUB_SCHEMA_URL + path
    logger.info(f"Fetching schema from {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        schema = response.json()
    except (requests.exceptions.RequestException, json.JSONDecodeError):
        message = PubSubErrorMessage(
            "Exception", "Failed to fetch schema from GitHub.", path
        )
        logger.error(f"{message.error_message} {url}")
    return schema


def fetch_survey_id(schema: dict) -> str:
    """
    Fetches the survey ID from the schema JSON.

    Parameters:
        schema (dict): the schema JSON.

    Returns:
        str: the survey ID.
    """
    try:
        survey_id = schema["properties"]["survey_id"]["enum"][0]
    except (KeyError, IndexError):
        message = PubSubErrorMessage(
            "SurveyIdError", "Failed to fetch survey_id from schema JSON.", "N/A"
        )
        logger.error(message.error_message)
        exit(1)
    return survey_id


def post_schema(schema: dict, survey_id: str, filepath: str) -> None:
    """
    Posts the schema to SDS.

    Parameters:
        schema (dict): the schema to be posted.
        survey_id (str): the survey ID.
        filepath (str): the path to the schema JSON.
    """
    logger.info(f"Posting schema for survey {survey_id}")
    url = f"{config.SDS_URL}{config.POST_SCHEMA_ENDPOINT}{survey_id}"
    response = http_manager.make_post_request(url, schema)
    if response.status_code != 200:
        PubSubErrorMessage(
            "SchemaPostError",
            f"Failed to post schema for survey {survey_id}",
            filepath,
        )
        logger.error(f"Failed to post schema for survey {survey_id}")
    else:
        logger.info(f"Schema: {filepath} posted for survey {survey_id}")


def split_filename(path: str) -> str:
    """
    Splits the filename without extension from the path.

    Parameters:
        path (str): the path to the schema JSON.

    Returns:
        str: the filename.
    """
    try:
        return Path(path).stem
    except Exception:
        message = PubSubErrorMessage(
            "Exception", "Failed to split filename from path.", path
        )
        logger.error(message.error_message)


def verify_version(filepath: str, schema: dict) -> bool:
    """
    Method to verify the schema version in the JSON matches the filename.

    Parameters:
        filename (str): the filename of the schema.
        schema (dict): the schema to be posted.

    Returns:
        bool: True if the schema version matches the filename.
    """
    logger.info(f"Verifying schema version for {filepath}")
    filename = split_filename(filepath)
    try:
        if schema["properties"]["schema_version"]["const"] == filename:
            return True
        else:
            message = PubSubErrorMessage(
                "SchemaVersionError",
                f"Schema version for {filepath} does not match. Expected {filename}, got {schema['properties']['schema_version']['const']}",
                filepath,
            )
            logger.error(message.error_message)
            exit(1)
    except KeyError:
        message = PubSubErrorMessage(
            "SchemaVersionError", f"Schema version not found in {filepath}.", filepath
        )
        logger.error(message.error_message)
        exit(1)


def check_duplicate_versions(schema: dict, survey_id: str) -> bool:
    """
    Method to call the schema_metadata endpoint and check that the schema_version for the new schema is not already present in SDS.

    Parameters:
        schema (dict): the schema to be posted.
        survey_id (str): the survey ID.

    Returns:
        bool: True if there are no duplicate versions, False otherwise.
    """
    schema_metadata = get_schema_metadata(survey_id)

    # If the schema_metadata endpoint returns a 404, then the survey is new and there are no duplicate versions.
    if schema_metadata.status_code == 404:
        return True

    new_schema_version = schema["properties"]["schema_version"]["const"]

    for version in schema_metadata.json():
        if new_schema_version == version["schema_version"]:
            message = PubSubErrorMessage(
                "SchemaVersionError",
                f"Schema version {new_schema_version} already exists for survey {survey_id}",
                "N/A",
            )
            logger.error(message.error_message)
            exit(1)
    return True


def get_schema_metadata(survey_id: str) -> requests.Response:
    """
    Method to call the schema_metadata endpoint and return the response for the survey.

    Parameters:
        survey_id (str): the survey_id of the schema.

    Returns:
        requests.Response: the response from the schema_metadata endpoint.
    """
    url = f"{config.SDS_URL}{config.GET_SCHEMA_METADATA_ENDPOINT}{survey_id}"
    response = http_manager.make_get_request(url)
    if response.status_code == 404:
        logger.debug(
            f"Schema metadata for survey {survey_id} not found, new survey added."
        )
        return response
    elif response.status_code != 200:
        message = PubSubErrorMessage(
            "SchemaMetadataError",
            f"Failed to fetch schema metadata for survey {survey_id}",
            "N/A",
        )
        logger.error(message.error_message)
        exit(1)
    return response
