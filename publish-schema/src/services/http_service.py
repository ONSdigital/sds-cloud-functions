import json
import logging

import google.oauth2.id_token
import requests
from config.config import CONFIG
from google.cloud import secretmanager
from pubsub.pub_sub_message import PubSubMessage
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logging = logging.getLogger(__name__)


class HTTPService:
    def __init__(self):
        self.session = self._setup_session()
        self.headers = self._generate_headers()

    def _setup_session(self) -> requests.Session:
        """
        Setup a http/s session.

        Parameters:
            None

        Returns:
            Session: a http/s session.
        """
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _generate_headers(self) -> dict[str, str]:
        """
        Create headers for authentication through SDS load balancer.

        Returns:
            dict[str, str]: the headers required for remote authentication.
        """
        headers = {}
        oauth_client_id = self._access_secret_version()
        auth_req = google.auth.transport.requests.Request()
        auth_token = google.oauth2.id_token.fetch_id_token(
            auth_req, audience=oauth_client_id
        )

        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }

        print(f"Headers: {headers}")

        return headers

    def make_post_request(self, url: str, data: dict) -> requests.Response:
        """
        Make a POST request to a specified URL.

        Parameters:
            url (str): the URL to send the POST request to.
            data (dict): the data to send in the POST request.

        Returns:
            requests.Response: the response from the POST request.
        """

        response = self.session.post(url, headers=self.headers, json=data)
        return response

    def make_get_request(self, url: str) -> requests.Response:
        """
        Make a GET request to a specified URL.

        Parameters:
            url (str): the URL to send the GET request to.

        Returns:
            requests.Response: the response from the GET request.
        """
        response = self.session.get(url, headers=self.headers)
        return response

    def _access_secret_version(self) -> str:
        """
        Access the secret version from Google Cloud Secret Manager.

        Returns:
            str: The OAuth client ID.
        """
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{CONFIG.PROJECT_ID}/secrets/{CONFIG.SECRET_ID}/versions/latest"
            response = client.access_secret_version(name=name)
            secret_data = response.payload.data.decode("UTF-8")
            secret_json = json.loads(secret_data)
            oauth_client_id = secret_json["web"]["client_id"]
            return oauth_client_id
        except Exception as e:
            message = PubSubMessage(
                "Exception",
                "Failed to access secret version from Secret Manager.",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            raise RuntimeError(message.message) from e


HTTP_SERVICE = HTTPService()
