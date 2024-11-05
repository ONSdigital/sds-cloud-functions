import base64
import sys
from cloudevents.http import CloudEvent
import functions_framework
from http_helper import generate_headers, setup_session
from config import Config
import requests
import json
import logging

logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    """
    Method to retrieve, verify, and publish a schema to SDS.
    
    Parameters:
        cloud_event (CloudEvent): the CloudEvent containing the message.
    """
    filepath = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    schema = fetch_raw_schema(filepath)

    if not verify_version(filepath, schema):
        logger.error("Stopping execution due to schema version mismatch.")
        return

    survey_id = fetch_survey_id(schema)
    response = post_schema(schema, survey_id)
    if response.status_code == 200:
        logger.info(f"Schema for survey {survey_id} posted successfully")
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

    url = f'https://raw.githubusercontent.com/ONSdigital/sds-prototype-schema/refs/heads/SDSS-823-schema-publication-automation-spike/{path}'
    logger.info(f"Fetching schema from {url}")
    response = requests.get(url)
    schema = response.content
    try:
        schema = json.loads(schema)
    except json.JSONDecodeError:
        logger.error(f"Failed to load schema JSON from {url}")
        logger.debug(schema)
        exit(1)

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
        f"{Config.API_URL}/v1/schema?survey_id={survey_id}",
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
        logger.error(f"Schema version for {filename} does not match. Expected {filename}, got {schema['properties']['schema_version']['const']}")
    return False