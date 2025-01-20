import base64
from cloudevents.http import CloudEvent
import functions_framework
from http_helper import generate_headers, setup_session
from config import Config
import requests
import json
import logging

logger = logging.getLogger(__name__)


@functions_framework.cloud_event
def publish_schema(cloud_event: CloudEvent) -> None:
    """
    Method to retrieve, verify, and publish a schema to SDS.

    Parameters:
        cloud_event (CloudEvent): the CloudEvent containing the message.
    """
    filepath = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")

    schema = fetch_raw_schema(filepath)

    if schema is None:
        logger.error("Stopping execution due to failed schema fetch.")
        return

    if not verify_version(filepath, schema):
        logger.error("Stopping execution due to schema version mismatch.")
        return

    survey_id = fetch_survey_id(schema)

    if not check_duplicate_versions(schema, survey_id):
        logger.error("Stopping execution due to duplicate schema version.")
        return

    response = post_schema(schema, survey_id)
    if response.status_code == 200:
        logger.info(f"Schema {filepath} posted successfully")
    else:
        logger.error(f"Failed to post schema for survey {survey_id}")
        logger.error(response.text)


def fetch_raw_schema(path) -> dict:
    """
    Method to fetch the schema from the ONSdigital GitHub repository.

    Parameters:
        path (str): the path to the schema JSON.

    Returns:
        dict: the schema JSON.
    """

    url = Config.GITHUB_URL + path
    logger.info(f"Fetching schema from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch schema from {url}")
        logger.error(e)
        return None

    try:
        schema = response.json()
    except json.JSONDecodeError as e:
        logger.error("Failed to decode schema JSON")
        logger.error(e)
        return None
    return schema


def fetch_survey_id(schema) -> str:
    """
    Method to fetch the survey ID from the schema JSON.

    Parameters:
        schema (dict): the schema JSON.
    """
    try:
        survey_id = schema["properties"]["survey_id"]["enum"][0]
    except KeyError:
        logger.error("Survey ID not found in schema")
        exit(1)
    return survey_id


def post_schema(schema, survey_id) -> requests.Response:
    """
    Method to post the schema to the API.

    Parameters:
        schema (dict): the schema to be posted.
        survey_id (str): the survey ID.

    Returns:
        requests.Response: the response from the post schema endpoint.
    """
    session = setup_session()
    headers = generate_headers()
    logger.info(f"Posting schema for survey {survey_id}")
    response = session.post(
        f"{Config.SDS_URL}{Config.POST_SCHEMA_ENDPOINT}{survey_id}",
        json=schema,
        headers=headers,
    )
    return response


def split_filename(path) -> str:
    """
    Method to split the filename without extension from the path.

    Parameters:
        path (str): the path to the schema JSON.

    Returns:
        str: the filename.
    """
    try:
        return path.split("/")[-1].split(".")[0]
    except IndexError:
        logger.error(f"Failed to split filename from {path}.")
        exit(1)


def verify_version(filepath, schema) -> bool:
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


def check_duplicate_versions(schema, survey_id) -> bool:
    """
    Method to call the schema_metadata endpoint and check that the schema_version for the new schema is not already present in SDS.

    Parameters:
        schema (dict): the schema to be posted.
        survey_id (str): the survey ID.

    Returns:
        bool: True if there are no duplicate versions, False otherwise.
    """
    logger.debug(f"Checking for duplicate schema versions for survey {survey_id}")

    logger.debug(f"Fetching schema metadata for survey {survey_id}")
    schema_metadata = get_schema_metadata(survey_id)

    new_schema_version = schema["properties"]["schema_version"]["const"]

    for version in schema_metadata:
        if new_schema_version == version["schema_version"]:
            logger.error(
                f"Schema version {new_schema_version} already exists for survey {survey_id}"
            )
            return False
    logger.info(
        f"Verified schema_version {new_schema_version} for survey {survey_id} is unique."
    )
    return True


def get_schema_metadata(survey_id) -> dict:
    """
    Method to call the schema_metadata endpoint and return the metadata for the survey.

    Parameters:
        survey_id (str): the survey_id of the schema.

    Returns:
        dict: the metadata for the survey.
    """

    session = setup_session()
    headers = generate_headers()
    try:
        response = session.get(
            f"{Config.SDS_URL}{Config.GET_SCHEMA_METADATA_ENDPOINT}{survey_id}",
            headers=headers,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch schema metadata for survey {survey_id}")
        logger.error(e)
        return None
    return response.json()
