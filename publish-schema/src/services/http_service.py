import logging

import google.oauth2.id_token
import requests
from services.secret_service import SECRET_SERVICE
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logging = logging.getLogger(__name__)


class HTTPService:
    def __init__(self):
        self.session = self._setup_session()
        self.sds_headers = self._generate_headers()

    @staticmethod
    def _setup_session() -> requests.Session:
        """
        Set up a http/s session.

        Returns:
            Session: a http/s session.
        """
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    @staticmethod
    def _generate_headers() -> dict[str, str]:
        """
        Create headers for authentication through SDS load balancer.

        Returns:
            dict[str, str]: the headers required for remote authentication.
        """
        oauth_client_id = SECRET_SERVICE.get_oauth_client_id()
        auth_req = google.auth.transport.requests.Request()
        auth_token = google.oauth2.id_token.fetch_id_token(
            auth_req, audience=oauth_client_id
        )

        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }

        return headers

    def make_post_request(self, url: str, data: dict) -> requests.Response:
        """
        Make a POST request to a specified URL.

        Parameters:
            url (str): the URL to send the POST request to.
            data (dict): the JSON data to send in the POST request.

        Returns:
            requests.Response: the response from the POST request.
        """

        response = self.session.post(url, headers=self.sds_headers, json=data)
        return response

    def make_get_request(self, url: str, sds_headers=None) -> requests.Response:
        """
        Make a GET request to a specified URL.

        Parameters:
            url (str): the URL to send the GET request to.
            sds_headers (bool): whether to include the SDS headers in the request (for SDS API).

        Returns:
            requests.Response: the response from the GET request.
        """
        response = self.session.get(
            url, headers=self.sds_headers if sds_headers else None
        )
        return response


HTTP_SERVICE = HTTPService()
