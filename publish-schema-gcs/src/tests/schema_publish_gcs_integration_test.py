import pytest
from sds_common.config.config import CONFIG
from sds_common.enums.buckets import Bucket
from sds_common.repositories.bucket_repository import BucketRepository
from sds_common.repositories.bucket_loader import bucket_loader
from sds_common.test_helpers.common_test_data import test_schema_subscriber_id_fail, test_schema_subscriber_id_success
from sds_common.test_helpers.integration_helpers import cleanup, pubsub_setup, inject_wait_time
from sds_common.test_helpers.pub_sub_helper import PubSubHelper


class SchemaPublishGcsIntegrationTest:
    @classmethod
    def setup_class(cls):
        cleanup()
        cls.schema_queue_pubsub_helper = PubSubHelper(
            CONFIG.PUBLISH_SCHEMA_QUEUE_TOPIC_ID
        )
        cls.schema_error_pubsub_helper = PubSubHelper(
            CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID
        )
        cls.schema_success_pubsub_helper = PubSubHelper(
            CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID
        )
        pubsub_setup(cls.schema_error_pubsub_helper, test_schema_subscriber_id_fail)
        pubsub_setup(
            cls.schema_success_pubsub_helper, test_schema_subscriber_id_success
        )
        inject_wait_time(3)  # Inject wait time to allow resources properly set up

    @pytest.mark.order(1)
    def test_publish_schema_to_gcs(self):
        """
        Test publishing a schema via GCS happy path.

        *We drop a valid schema file into the GCS schema publish bucket
        *We poll the schema_success_topic to check if the schema was published.
        *We assert that the schema was published successfully.

        """
        bucket_repository = BucketRepository(bucket_loader.fetch_bucket(Bucket.SCHEMA_PUBLISH_BUCKET))
        bucket_repository.upload_file_from_path("tests/test_data/test_schema_success.json")