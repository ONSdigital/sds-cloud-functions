import json

from config.config import CONFIG
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import secretmanager
from utilities.utils import raise_error


class SecretService:
    def __init__(self):
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = CONFIG.PROJECT_ID
        self.secret_id = CONFIG.SECRET_ID

    def get_oauth_client_id(self) -> str | None:
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
            raise_error(
                "KeyError",
                "OAuth client ID not found in secret.",
                "N/A",
            )

    def _get_secret_version(self) -> str | None:
        """
        Access the latest secret version from Google Cloud Secret Manager.

        Returns:
            str: The Secret value.
        """
        try:
            name = (
                f"projects/{self.project_id}/secrets/{self.secret_id}/versions/latest"
            )
            response = self.client.access_secret_version(name=name)
            return response.payload.data.decode("UTF-8")
        except (GoogleAPICallError, RetryError):
            raise_error(
                "SecretError",
                "Failed to access secret version from Google Cloud Secret Manager.",
                "N/A",
            )


SECRET_SERVICE = SecretService()
