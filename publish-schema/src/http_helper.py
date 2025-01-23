import google.oauth2.id_token
import requests
from config import config
from google.cloud import secretmanager
from logging_config import logging
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logging = logging.getLogger(__name__)


def setup_session() -> requests.Session:
    """
    Setup a http/s session to facilitate testing.

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


def generate_headers() -> dict[str, str]:
    """
    Create headers for authentication through SDS load balancer.

    Parameters:
        None

    Returns:
        dict[str, str]: the headers required for remote authentication.
    """
    headers = {}
    oauth_client_id = access_secret_version()
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


def access_secret_version() -> str:
    """
    Access the secret version from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{config.PROJECT_ID}/secrets/{config.SECRET_ID}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")["web"]["client_id"]
