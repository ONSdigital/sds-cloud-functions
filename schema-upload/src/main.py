import base64

from cloudevents.http import CloudEvent
import functions_framework
from http_helper import generate_headers, setup_session
from config import Config
import requests
import json
import logging

logger = logging.getLogger(__name__)


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    payload = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    schema = fetch_raw_schema(payload)
    survey_id = fetch_survey_id(schema)
    post_schema(schema, survey_id)


def fetch_raw_schema(path):
    url = f'https://raw.githubusercontent.com/ONSdigital/sds-prototype-schema/refs/heads/SDSS-823-schema-publication-automation-spike/{path}'
    print(f'Fetching schema from {url}')
    response = requests.get(url)
    schema = response.content
    schema = json.loads(schema)

    return schema

def fetch_survey_id(schema) -> str:
    return schema["properties"]["survey_id"]["enum"][0]


def post_schema(schema, survey_id):
    session = setup_session()
    headers = generate_headers()
    print(f"Posting schema for survey {survey_id}")
    response = session.post(
        f"{Config.API_URL}/v1/schema?survey_id={survey_id}",
        json=schema,
        headers=headers,
    )
    if response.status_code == 200:
        print(f"Schema for survey {survey_id} posted successfully")
    else:
        print(f"Failed to post schema for survey {survey_id}")
        print(response.text)
