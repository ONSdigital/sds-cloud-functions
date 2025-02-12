import json
import time

from google.cloud import storage
from src.tests.helpers.bucket_helpers import (
    delete_blobs_with_test_survey_id,
)
from src.tests.helpers.bucket_loader import bucket_loader
from src.tests.helpers.firebase_loader import firebase_loader
from src.tests.helpers.firestore_helpers import (
    perform_delete_on_collection_with_test_survey_id,
)
from src.tests.helpers.pub_sub_helper import PubSubHelper
from src.tests.test_data.schema_test_data import test_survey_id

storage_client = storage.Client()


def load_json(filepath: str) -> dict:
    """
    Method to load json from a file.

    Parameters:
        filepath: string specifying the location of the file to be loaded.

    Returns:
        dict: the json object from the specified file.
    """
    with open(filepath) as f:
        return json.load(f)


def cleanup() -> None:
    """
    Method to clean up all schema test data created in buckets/FireStore.
    Should be run before and after test to account for test failures.
    """

    delete_blobs_with_test_survey_id(bucket_loader.get_schema_bucket(), test_survey_id)

    client = firebase_loader.get_client()

    perform_delete_on_collection_with_test_survey_id(
        client, firebase_loader.get_schemas_collection(), test_survey_id
    )


def pubsub_setup(pubsub_helper: PubSubHelper, subscriber_id: str) -> None:
    """Creates any subscribers that may be used in tests"""
    pubsub_helper.try_create_subscriber(subscriber_id)


def pubsub_teardown(pubsub_helper: PubSubHelper, subscriber_id: str) -> None:
    """Deletes subscribers that may have been used in tests"""
    pubsub_helper.try_delete_subscriber(subscriber_id)


def pubsub_purge_messages(pubsub_helper: PubSubHelper, subscriber_id: str) -> None:
    """Purge any messages that may have been sent to a subscriber"""
    pubsub_helper.purge_messages(subscriber_id)


def inject_wait_time(seconds: int) -> None:
    """
    Method to inject a wait time into the test to allow resources properly spin up and tear down.

    Parameters:
        seconds: the number of seconds to wait

    Returns:
        None
    """
    time.sleep(seconds)


def is_json_response(response):
    try:
        response.json()
        return True
    except Exception:
        return False


def poll_subscription(pubsub_helper, subscriber_id, timeout=45) -> list[dict] | None:
    """
    Polls a subscription for messages until the timeout is reached.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = pubsub_helper.pull_and_acknowledge_messages(subscriber_id)
        if response:
            return response
        time.sleep(3)
    return None
