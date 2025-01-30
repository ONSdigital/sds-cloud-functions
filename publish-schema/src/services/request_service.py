import json
import logging

import requests
from config.config import CONFIG
from pubsub.pub_sub_message import PubSubMessage
from schema.schema import Schema
from services.http_service import HTTP_SERVICE

logger = logging.getLogger(__name__)


class RequestService:
    def get_schema_metadata(self, survey_id: str) -> requests.Response:
        """
        Method to call the schema_metadata endpoint and return the response for the survey.

        Parameters:
            survey_id (str): the survey_id of the schema.

        Returns:
            requests.Response: the response from the schema_metadata endpoint.
        """
        url = f"{CONFIG.SDS_URL}{CONFIG.GET_SCHEMA_METADATA_ENDPOINT}{survey_id}"
        response = HTTP_SERVICE.make_get_request(url)
        # If the response status code is not 200 or 404 (new survey added), raise an error
        if response.status_code != 200 or response.status_code != 404:
            message = PubSubMessage(
                "SchemaMetadataError",
                f"Failed to fetch schema metadata for survey {survey_id}",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            raise RuntimeError(message.message)
        return response

    def post_schema(self, schema: Schema) -> None:
        """
        Posts the schema to SDS.

        Parameters:
            schema (dict): the schema to be posted.
            survey_id (str): the survey ID.
            filepath (str): the path to the schema JSON.
        """
        survey_id = schema.get_survey_id()
        filepath = schema.get_filepath()

        logger.info(f"Posting schema for survey {survey_id}")
        url = f"{CONFIG.SDS_URL}{CONFIG.POST_SCHEMA_ENDPOINT}{survey_id}"
        response = HTTP_SERVICE.make_post_request(url, schema)
        if response.status_code != 200:
            message = PubSubMessage(
                "SchemaPostError",
                f"Failed to post schema for survey {survey_id}",
                filepath,
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            raise RuntimeError(message.message)
        else:
            message = PubSubMessage(
                "SchemaPostSuccess",
                f"Schema {filepath} posted for survey {survey_id}",
                filepath,
                CONFIG.PUBLISH_SCHEMA_SUCCESS_TOPIC_ID
            )
            logger.info(message.message)

    def fetch_raw_schema(self, path: str) -> dict:
        """
        Fetches the schema from the ONSdigital GitHub repository.

        Parameters:
            path (str): the path to the schema JSON.

        Returns:
            dict: the schema JSON.
        """
        url = CONFIG.GITHUB_SCHEMA_URL + path
        logger.info(f"Fetching schema from {url}")
        response = HTTP_SERVICE.make_get_request(url)

        if response.status_code != 200:
            message = PubSubMessage(
                "SchemaFetchError", "Failed to fetch schema from GitHub.", path, CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID
            )
            raise RuntimeError(message.message)
        schema = self._decode_json_response(response)
        return schema

    def _decode_json_response(self, response: requests.Response) -> dict:
        """
        Decode the JSON response from a requests.Response object.

        Parameters:
            response (requests.Response): the response object to decode.

        Returns:
            dict: the decoded JSON response.
        """
        try:
            decoded_response = response.json()
        except json.JSONDecodeError as e:
            message = PubSubMessage(
                "JSONDecodeError",
                "Failed to decode JSON response.",
                "N/A",
                CONFIG.PUBLISH_SCHEMA_ERROR_TOPIC_ID,
            )
            raise RuntimeError(message.message) from e
        return decoded_response


REQUEST_SERVICE = RequestService()
