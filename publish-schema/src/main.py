import base64
from cloudevents.http import CloudEvent
import functions_framework
from http_helper import generate_headers, setup_session
from config import config
import requests
import json
import logging
from pathlib import Path

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

    if not verify_version(filepath, schema):
        logger.error("Stopping execution due to schema version mismatch.")
        return

    survey_id = fetch_survey_id(schema)

    if not check_duplicate_versions(schema, survey_id):
        logger.error("Stopping execution due to duplicate schema version.")
        return

    post_schema(schema, survey_id)


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
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch schema from {url} - exiting...")
        logger.error(e)
        exit(1)
    except json.JSONDecodeError as e:
        logger.error("Failed to decode schema JSON. Exiting...")
        logger.error(e)
        exit(1)
    return schema


def fetch_survey_id(schema: dict) -> str:
    """
    Fetches the survey ID from the schema JSON.

    Parameters:
        schema (dict): the schema JSON.
    """
    try:
        survey_id = schema["properties"]["survey_id"]["enum"][0]
    except KeyError:
        logger.error("Survey ID not found in schema")
        exit(1)
    except IndexError:
        logger.error("Survey ID not found in schema")
        exit(1)
    return survey_id


def post_schema(schema: dict, survey_id: str) -> None:
    """
    Posts the schema to SDS.

    Parameters:
        schema (dict): the schema to be posted.
        survey_id (str): the survey ID.
    """
    session = setup_session()
    headers = generate_headers()
    logger.info(f"Posting schema for survey {survey_id}")
    try:
        response = session.post(
            f"{config.SDS_URL}{config.POST_SCHEMA_ENDPOINT}{survey_id}",
            json=schema,
            headers=headers,
        )
        response.raise_for_status()
        logger.info(f"Schema posted for survey {survey_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to post schema for survey {survey_id}. Status code: {response.status_code}")
        logger.error(e)
        exit(1)


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
    except Exception as e:
        logger.error(f"Failed to split filename from {path}.")
        logger.error(e)
        exit(1)


def verify_version(filepath: str, schema: dict) -> bool:
    """
    Method to verify the schema version in the JSON matches the filename.

    Parameters:
        filename (str): the filename of the schema.
        schema (dict): the schema to be posted.

    Returns:
        bool: True if the schema version matches the filename, False otherwise.
    """
    logger.info(f"Verifying schema version for {filepath}")
    filename = split_filename(filepath)
    if schema["properties"]["schema_version"]["const"] == filename:
        logger.info(f"Schema version for {filename} verified.")
        return True
    else:
        logger.error(
            f"Schema version for {filepath} does not match. Expected {filename}, got {schema['properties']['schema_version']['const']}"
        )
    return False


def check_duplicate_versions(schema: dict, survey_id: str) -> bool:
    """
    Method to call the schema_metadata endpoint and check that the schema_version for the new schema is not already present in SDS.

    Parameters:
        schema (dict): the schema to be posted.
        survey_id (str): the survey ID.

    Returns:
        bool: True if there are no duplicate versions, False otherwise.
    """
    logger.info(f"Checking for duplicate schema versions for survey {survey_id}")

    logger.debug(f"Fetching schema metadata for survey {survey_id}")
    schema_metadata = get_schema_metadata(survey_id)

    if schema_metadata.status_code == 404:
        return True

    new_schema_version = schema["properties"]["schema_version"]["const"]

    for version in schema_metadata.json():
        if new_schema_version == version["schema_version"]:
            logger.error(
                f"Schema version {new_schema_version} already exists for survey {survey_id}"
            )
            return False
    logger.info(
        f"Verified schema_version {new_schema_version} for survey {survey_id} is not present in SDS. Continuing..."
    )
    return True


def get_schema_metadata(survey_id: str) -> requests.Response:
    """
    Method to call the schema_metadata endpoint and return the response for the survey.

    Parameters:
        survey_id (str): the survey_id of the schema.

    Returns:
        requests.Response: the response from the schema_metadata endpoint.
    """
    session = setup_session()
    headers = generate_headers()
    try:
        response = session.get(
            f"{config.SDS_URL}{config.GET_SCHEMA_METADATA_ENDPOINT}{survey_id}",
            headers=headers,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if response.status_code == 404:
            logger.debug(f"Schema metadata for survey {survey_id} not found, new survey added.")
            return response
        else:
            logger.error(f"Failed to fetch schema metadata for survey {survey_id}")
            logger.error(e)
            exit(1)
    return response
