import json
from google.cloud import secretmanager
from config.config import CONFIG
from pubsub.pub_sub_message import PubSubMessage
from pubsub.pub_sub_publisher import PUB_SUB_PUBLISHER
from google.api_core.exceptions import GoogleAPICallError, RetryError


class SecretService:
    def __init__(self):
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = CONFIG.PROJECT_ID
        self.secret_id = CONFIG.SECRET_ID

    def get_oauth_client_id(self) -> str:
        """
        Get the OAuth client ID for authenticating with SDS.

        Returns:
            str: the OAuth client ID.
        """
        try:
            secret = self._get_secret_version()
            secret_json = json.loads(secret)
            return secret_json["web"]["client_id"]
        except KeyError:
            message = PubSubMessage(
                "KeyError",
                "OAuth client ID not found in secret.",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            PUB_SUB_PUBLISHER.send_message(message)
            raise RuntimeError(message.message) from None

    def _get_secret_version(self) -> str:
        """
        Access the latest secret version from Google Cloud Secret Manager.

        Returns:
            str: The Secret value.
        """
        try:
            name = f"projects/{self.project_id}/secrets/{self.secret_id}/versions/latest"
            response = self.client.access_secret_version(name=name)
            return response.payload.data.decode("UTF-8")
        except (GoogleAPICallError, RetryError):
            message = PubSubMessage(
                "SecretError",
                "Failed to access secret version from Google Cloud Secret Manager.",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            PUB_SUB_PUBLISHER.send_message(message)
            raise RuntimeError(message.message) from None


SECRET_SERVICE = SecretService()
