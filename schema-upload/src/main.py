import base64

from cloudevents.http import CloudEvent
import functions_framework
from http_hepler import generate_headers, setup_session
from config import config
import requests
import json

logger = logging.getLogger(__name__)


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    payload = json.loads(base64.b64decode(cloud_event['data']).decode('utf-8'))
    schema = fetch_raw_schema(payload['path'])
    post_schema(schema)


def fetch_raw_schema(path):
    response = requests.get(f'"https://raw.githubusercontent.com/ONSdigital/sds-schema-definitions/refs/heads/main"{path}')
    schema = response.content

    return schema


def post_schema(data):
    session = setup_session()
    headers = generate_headers()

    session.post(
        f"{config.API_URL}/v1/schema?survey_id={data['survey_id']}",
        json=data,
        headers=headers,
    )
