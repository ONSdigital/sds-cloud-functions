import json

import requests
from config.config import CONFIG
from config.logging_config import logging
from models.error_models import (
    SchemaJSONDecodeError,
    SchemaFetchError,
    SchemaMetadataError,
    SchemaPostError,
)
from schema.schema import Schema
from services.http_service import HTTP_SERVICE

logger = logging.getLogger(__name__)


class RequestService:
    @staticmethod
    def get_schema_metadata(survey_id: str) -> requests.Response:
        """
        Call the GET schema_metadata SDS endpoint and return the response for the survey.

        Parameters:
            survey_id (str): the survey_id of the schema.

        Returns:
            requests.Response: the response from the schema_metadata endpoint.
        """
        url = f"{CONFIG.SDS_URL}{CONFIG.GET_SCHEMA_METADATA_ENDPOINT}{survey_id}"
        response = HTTP_SERVICE.make_get_request(url, sds_headers=True)
        # If the response status code is 404, a new survey is being onboarded.
        if response.status_code != 200 and response.status_code != 404:
            raise SchemaMetadataError(survey_id)
        return response

    @staticmethod
    def post_schema(schema: Schema) -> None:
        """
        Post the schema to SDS.

        Parameters:
            schema (Schema): the schema to be posted.
        """
        logger.info(f"Posting schema for survey {schema.survey_id}")
        url = f"{CONFIG.SDS_URL}{CONFIG.POST_SCHEMA_ENDPOINT}{schema.survey_id}"
        response = HTTP_SERVICE.make_post_request(url, schema.json)
        if response.status_code != 200:
            raise SchemaPostError(schema.filepath)
        else:
            logger.info(
                f"Schema {schema.filepath} posted for survey {schema.survey_id}"
            )

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
            raise SchemaFetchError(path, response.status_code, url)
        schema = self._decode_json_response(response)
        return schema

    @staticmethod
    def _decode_json_response(response: requests.Response) -> dict | None:
        """
        Decode the JSON response from a requests.Response object.

        Parameters:
            response (requests.Response): the response object to decode.

        Returns:
            dict: the decoded JSON response.
        """
        try:
            decoded_response = response.json()
            return decoded_response
        except json.JSONDecodeError:
            raise SchemaJSONDecodeError("N/A")


REQUEST_SERVICE = RequestService()
