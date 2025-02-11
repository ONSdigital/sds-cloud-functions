import json
import time

from config.schema_config import CONFIG
from google.cloud import pubsub_v1


class PubSubHelper:
    def __init__(self, topic_id: str) -> None:
        self.subscriber_client = pubsub_v1.SubscriberClient()
        self.publisher_client = pubsub_v1.PublisherClient()
        self.topic_id = topic_id

    def _topic_exists(self, topic_path: str) -> bool:
        """
        Returns True if the topic exists otherwise returns False.
        """
        try:
            self.publisher_client.get_topic(request={"topic": topic_path})
            return True
        except Exception:
            return False

    def try_create_subscriber(self, subscriber_id: str, attempts: int = 5) -> None:
        """
        Creates a subscriber with a unique subscriber id if one does not already exist.

        Parameters:
        subscriber_id: the unique id of the subscriber being created.
        """
        topic_path = self.publisher_client.topic_path(CONFIG.PROJECT_ID, self.topic_id)

        subscription_path = self.subscriber_client.subscription_path(
            CONFIG.PROJECT_ID, subscriber_id
        )

        if not self._subscription_exists(subscriber_id):
            self.subscriber_client.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "enable_message_ordering": True,
                }
            )

        while attempts != 0:
            if self._wait_and_check_subscription_exists(subscriber_id):
                return

            attempts -= 1

        print(f"Fail to create subscriber. Subscription path: {subscription_path}")

    def publish_message(self, message: str) -> None:
        """
        Publishes a message to a topic.

        Parameters:
        message: the message to be published.
        """
        topic_path = self.publisher_client.topic_path(CONFIG.PROJECT_ID, self.topic_id)

        self.publisher_client.publish(topic_path, data=message.encode("utf-8"))

    def pull_and_acknowledge_messages(self, subscriber_id: str) -> list[dict] | None:
        """
        Pulls all messages published to a topic via a subscriber.

        Parameters:
        subscriber_id: the unique id of the subscriber being created.
        """
        subscription_path = self.subscriber_client.subscription_path(
            CONFIG.PROJECT_ID, subscriber_id
        )
        NUM_MESSAGES = 5

        response = self.subscriber_client.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES},
        )

        message_count = len(response.received_messages)

        if message_count == 0:
            return None

        messages = []
        ack_ids = []

        for received_message in response.received_messages:
            messages.append(self.format_received_message_data(received_message))
            ack_ids.append(received_message.ack_id)

        self.subscriber_client.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )

        return messages

    def purge_messages(self, subscriber_id: str) -> None:
        """
        Purges all messages published to a subscriber by seeking through future timestamp.

        Parameters:
        subscriber_id: the unique id of the subscriber being created.
        """
        subscription_path = self.subscriber_client.subscription_path(
            CONFIG.PROJECT_ID, subscriber_id
        )

        self.subscriber_client.seek(
            request={"subscription": subscription_path, "time": "2999-01-01T00:00:00Z"}
        )

    def format_received_message_data(self, received_message) -> dict:
        """
        Formats a messages received from a topic.

        Parameters:
        received_message: The message received from the topic.
        """
        return json.loads(
            received_message.message.data.decode("utf-8").replace("'", '"')
        )

    def try_delete_subscriber(self, subscriber_id: str, attempts: int = 5) -> None:
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = self.subscriber_client.subscription_path(
            CONFIG.PROJECT_ID, subscriber_id
        )

        if self._subscription_exists(subscriber_id):
            with subscriber:
                subscriber.delete_subscription(
                    request={"subscription": subscription_path}
                )
        while attempts != 0:
            if self._wait_and_check_subscription_deleted(subscriber_id):
                return

            attempts -= 1

        print(f"Fail to delete subscriber. Subscription path: {subscription_path}")

    def _subscription_exists(self, subscriber_id: str) -> bool:
        """
        Checks a subscription exists.

        Parameters:
        subscriber_id: the unique id of the subscriber being checked.
        """
        subscription_path = self.subscriber_client.subscription_path(
            CONFIG.PROJECT_ID, subscriber_id
        )

        try:
            self.subscriber_client.get_subscription(
                request={"subscription": subscription_path}
            )
            return True
        except Exception:
            return False

    def _wait_and_check_subscription_exists(
            self,
            subscriber_id: str,
            attempts: int = 5,
            backoff: int = 0.5,
    ) -> bool:
        """
        Waits for a subscription to be created and checks if it exists.

        Parameters:
        subscriber_id: the unique id of the subscriber being checked.
        attempts: the number of attempts to check if the subscription exists.
        backoff: the time to wait between attempts.
        """
        while attempts != 0:
            if self._subscription_exists(subscriber_id):
                return True

            attempts -= 1
            time.sleep(backoff)
            backoff += backoff

        return False

    def _wait_and_check_subscription_deleted(
            self,
            subscriber_id: str,
            attempts: int = 5,
            backoff: int = 0.5,
    ) -> bool:
        """
        Waits for a subscription to be created and checks if it is deleted.

        Parameters:
        subscriber_id: the unique id of the subscriber being checked.
        attempts: the number of attempts to check if the subscription is deleted.
        backoff: the time to wait between attempts.
        """
        while attempts != 0:
            if not self._subscription_exists(subscriber_id):
                return True

            attempts -= 1
            time.sleep(backoff)
            backoff += backoff

        return False
