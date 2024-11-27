from typing import Literal

from models.dataset_models import DatasetMetadataWithoutId


class DocumentVersionService:
    @staticmethod
    def calculate_survey_version(
            document_current_version: dict | DatasetMetadataWithoutId,
            version_key: Literal["sds_dataset_version", "sds_schema_version"],
    ) -> int:
        """
        Calculates the next version number of a document based on a version key, returning 1 by default if no document exists.

        Parameters:
        document_current_version: document that the version is being calculated from
        version_key: the key being accessed to find out the document version.
        """
        if document_current_version is None:
            return 1

        if version_key not in document_current_version:
            raise RuntimeError("Document must contain version key")

        return document_current_version[version_key] + 1


    @staticmethod
    def calculate_previous_version(
            document_current_version: DatasetMetadataWithoutId,
            version_key: Literal["sds_dataset_version"],) -> int:
        """
        Calculates the previous version number of a document based on a version key
        Any return of < 1 value indicates an invalid previous version number and should block subsequent process
        Parameters:
        document_current_version: document that the version is being calculated from
        version_key: the key being accessed to find out the document version.
        """
        if document_current_version is None:
            raise RuntimeError(
                "Current version document is not found in calculate previous version service"
            )

        if version_key not in document_current_version:
            raise RuntimeError(
                "Document must contain version key to calculate previous version"
            )

        return document_current_version[version_key] - 1