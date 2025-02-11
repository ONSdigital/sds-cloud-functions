from unittest import TestCase
from src.config.schema_config import CONFIG
from src.tests.helpers.integration_helpers import (
    cleanup,
    pubsub_setup,
    pubsub_teardown,
    pubsub_purge_messages,
    inject_wait_time
)
from src.tests.helpers.pub_sub_helper import PubSubHelper
from src.tests.test_data.schema_test_data import test_schema_subscriber_id


class SchemaPublishErrorIntegrationTest(TestCase):
    @classmethod
    def setup_class(self):
        cleanup()
        self.schema_queue_pubsub_helper = PubSubHelper(CONFIG.PUBLISH_SCHEMA_QUEUE_TOPIC_ID)
        self.schema_error_pubsub_helper = PubSubHelper(CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID)
        self.schema_success_pubsub_helper = PubSubHelper(CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID)
        pubsub_setup(self.schema_queue_pubsub_helper, test_schema_subscriber_id)
        pubsub_setup(self.schema_error_pubsub_helper, test_schema_subscriber_id)
        pubsub_setup(self.schema_success_pubsub_helper, test_schema_subscriber_id)
        inject_wait_time(3)  # Inject wait time to allow resources properly set up

    @classmethod
    def teardown_class(self) -> None:
        cleanup()
        inject_wait_time(3) # Inject wait time to allow all message to be processed
        pubsub_purge_messages(self.schema_queue_pubsub_helper, test_schema_subscriber_id)
        pubsub_purge_messages(self.schema_error_pubsub_helper, test_schema_subscriber_id)
        pubsub_purge_messages(self.schema_success_pubsub_helper, test_schema_subscriber_id)
        pubsub_teardown(self.schema_queue_pubsub_helper, test_schema_subscriber_id)
        pubsub_teardown(self.schema_error_pubsub_helper, test_schema_subscriber_id)
        pubsub_teardown(self.schema_success_pubsub_helper, test_schema_subscriber_id)

    # def test_publish_schema_success(self):
    #     """
    #     Test the publish-schema Cloud Function by dropping a message onto the schema-publish-queue.
    #
    #     * We drop a message containing the filepath to a valid schema onto the queue.
    #     * We wait for the message to be processed.
    #     * We poll the schema_success_topic to check if the schema was published.
    #     * We assert that the schema was published successfully.
    #     """
    #     # publish the schema path to the queue topic
    #     self.schema_queue_pubsub_helper.publish_message(success_filepath)
    #
    #     # check for every 3 seconds for 1 minute if the message has been processed
    #     message = poll_subscription(self.schema_success_pubsub_helper, test_schema_subscriber_id)
    #
    #     # assert that the message was processed
    #     assert message is not None
    #     # assert that the first messages contains a guid in the json
    #     assert "guid" in message

    def test_test(self):
        assert 1 == 1
