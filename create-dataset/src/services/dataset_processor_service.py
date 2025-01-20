import uuid
from datetime import datetime

from config.config import config
from config.logging_config import logging
from models.dataset_models import (
    DatasetMetadata,
    DatasetMetadataWithoutId,
    RawDataset,
    RawDatasetWithoutData,
    UnitDataset,
)
from repository.dataset_firebase_repository import DatasetFirebaseRepository
from services.dataset_writer_service import DatasetWriterService
from services.document_version_service import DocumentVersionService

logger = logging.getLogger(__name__)


class DatasetProcessorService:
    def __init__(self) -> None:
        self.dataset_repository = DatasetFirebaseRepository()
        self.dataset_writer_service = DatasetWriterService(self.dataset_repository)

    def process_raw_dataset(self, filename: str, raw_dataset: RawDataset) -> None:
        """
        Processes the incoming create-dataset.
        Parameters:
        filename (str): the filename of the json containing the create-dataset data
        raw_dataset (RawDatasetWithMetadata): new create-dataset to be processed
        """
        logger.info("Processing new create-dataset...")
        logger.debug(f"Dataset being processed: {raw_dataset}")

        new_dataset_unit_data_collection = raw_dataset.pop("data")
        dataset_id = str(uuid.uuid4())

        dataset_metadata_without_id = self._add_metadata_to_new_dataset(
            raw_dataset, filename, new_dataset_unit_data_collection
        )
        unit_data_collection_with_metadata = self._add_metadata_to_unit_data_collection(
            dataset_id, dataset_metadata_without_id, new_dataset_unit_data_collection
        )
        extracted_unit_data_identifiers = self._extract_identifiers_from_unit_data(
            new_dataset_unit_data_collection
        )

        dataset_publish_response = self.dataset_writer_service.perform_dataset_write(
            dataset_id,
            dataset_metadata_without_id,
            unit_data_collection_with_metadata,
            extracted_unit_data_identifiers,
        )

        self.dataset_writer_service.try_publish_dataset_metadata_to_topic(
            dataset_publish_response
        )

        self._determine_deletion_of_previous_version_dataset(
            dataset_metadata_without_id["survey_id"],
            dataset_metadata_without_id["period_id"],
            dataset_metadata_without_id["sds_dataset_version"],
        )

    def get_dataset_metadata_collection(
            self, survey_id: str, period_id: str
    ) -> list[DatasetMetadata]:
        """
        Gets the collection of create-dataset metadata associated with a specific survey and period id.
        Parameters:
        survey_id (str): survey id of the collection.
        period_id (str): period id of the collection.
        """
        return self.dataset_repository.get_dataset_metadata_collection(
            survey_id, period_id
        )

    def _add_metadata_to_new_dataset(
            self,
            raw_dataset: RawDatasetWithoutData,
            filename: str,
            dataset_unit_data_collection: list[object],
    ) -> DatasetMetadataWithoutId:
        """
        Returns a copy of the create-dataset with added metadata.
        Parameters:
        raw_dataset (RawDatasetWithoutData): the original create-dataset without the data object.
        filename (str): the filename of the json containing the create-dataset data
        dataset_unit_data_collection (list[object]): collection of unit data in the new create-dataset
        """
        logger.info("Adding metadata to new create-dataset...")

        dataset_metadata_without_id = {
            "survey_id": raw_dataset["survey_id"],
            "period_id": raw_dataset["period_id"],
            "form_types": raw_dataset["form_types"],
            "filename": filename,
            "sds_published_at": str(
                datetime.now().strftime(config.TIME_FORMAT)
            ),
            "total_reporting_units": len(dataset_unit_data_collection),
            "sds_dataset_version": self._calculate_next_dataset_version(
                raw_dataset["survey_id"], raw_dataset["period_id"]
            ),
        }

        # Add optional field "title" into metadata if it exists in the raw dataset
        if "title" in raw_dataset:
            dataset_metadata_without_id["title"] = raw_dataset["title"]

        logger.info("Metadata added to new create-dataset successfully.")

        return dataset_metadata_without_id

    def _calculate_next_dataset_version(self, survey_id: str, period_id: str) -> int:
        """
        Calculates the next sds_dataset_version from a single create-dataset from firestore with a specific survey_id.
        Parameters:
        survey_id: survey_id of the specified create-dataset.
        period_id: period_id of the specified create-dataset.
        """
        datasets_result = (
            self.dataset_repository.get_latest_dataset_with_survey_id_and_period_id(
                survey_id, period_id
            )
        )

        return DocumentVersionService.calculate_survey_version(
            datasets_result, "sds_dataset_version"
        )

    def _calculate_previous_dataset_version_from_firestore(
            self, survey_id: str, period_id: str
    ) -> int:
        """
        Calculates the previous sds_dataset_version from a single create-dataset from firestore with a specific survey_id.
        Parameters:
        survey_id: survey_id of the specified create-dataset.
        period_id: period_id of the specified create-dataset.
        """
        datasets_result = (
            self.dataset_repository.get_latest_dataset_with_survey_id_and_period_id(
                survey_id, period_id
            )
        )

        return DocumentVersionService.calculate_previous_version(
            datasets_result, "sds_dataset_version"
        )

    def _add_metadata_to_unit_data_collection(
            self,
            dataset_id: str,
            transformed_dataset_metadata: DatasetMetadataWithoutId,
            raw_dataset_unit_data_collection: list[object],
    ) -> list[UnitDataset]:
        """
        Transforms the new unit data to a new format for storing in firestore.
        Parameters:
        dataset_id (str): dataset_id for the new create-dataset.
        transformed_dataset_metadata (DatasetMetadataWithoutId): the create-dataset metadata without id
        raw_dataset_unit_data_collection (list[object]): list of unit data to be transformed
        """
        logger.info("Adding metadata to unit data collection...")

        unit_data_collection_with_metadata = [
            self._add_metatadata_to_unit_data_item(
                dataset_id, transformed_dataset_metadata, item
            )
            for item in raw_dataset_unit_data_collection
        ]

        logger.info("Metadata added to unit data collection transformed successfully.")
        logger.debug(
            f"Metadata added to unit data collection for create-dataset with id: {dataset_id}"
        )

        return unit_data_collection_with_metadata

    def _add_metatadata_to_unit_data_item(
            self,
            dataset_id: str,
            transformed_dataset_metadata: DatasetMetadataWithoutId,
            unit_data_item: object,
    ) -> UnitDataset:
        """
        Transforms a unit data item to a new format for storing in firestore.
        Parameters:
        dataset_id (str): dataset_id for the new create-dataset.
        transformed_dataset_metadata (DatasetMetadataWithoutId): the create-dataset metadata without id
        unit_data_item (object): unit data item to be transformed
        """
        return {
            "dataset_id": dataset_id,
            "survey_id": transformed_dataset_metadata["survey_id"],
            "period_id": transformed_dataset_metadata["period_id"],
            "form_types": transformed_dataset_metadata["form_types"],
            "data": unit_data_item["unit_data"],
        }

    def _extract_identifiers_from_unit_data(
            self, raw_dataset_unit_data_collection: list[object]
    ) -> list:
        """
        Extracts all identifiers from unit data to store in a separate list
        Parameters:
        raw_dataset_unit_data_collection (list[object]): list of unit data containing identifier
        """
        logger.info("Extracting identifiers from unit data...")

        extracted_unit_data_identifiers = [
            item["identifier"] for item in raw_dataset_unit_data_collection
        ]

        logger.info("identifiers are extracted and stored successfully.")
        logger.debug(f"Extracted identifiers: {extracted_unit_data_identifiers}")

        return extracted_unit_data_identifiers

    def _determine_deletion_of_previous_version_dataset(
            self,
            current_dataset_survey_id: str,
            current_dataset_period_id: str,
            current_dataset_version: int,
    ) -> None:
        """
        Determine whether to delete the previous version of create-dataset.
        Retention flag either retain all datasets (True) or delete only the latest previous create-dataset (False).
        This flow should be present until an updated data retention policy is formulated (Card SDSS-207).
        Deletion of previous version of create-dataset happens when all of the following criteria are satisfied:
        1. Retention flag is off (False)
        2. The current version of create-dataset is > 1
        3. The current create-dataset is successfully added to FireStore
        Parameters:
        current_dataset_survey_id (str): Survey id of current processing create-dataset
        current_dataset_period_id (str): Period id of current processing create-dataset
        current_dataset_version (int): Version of current processing create-dataset
        """
        logger.info("Determining whether to delete previous version of create-dataset...")

        # Defensively set retention flag to True unless config explicitly stated as False
        retention_flag = True
        if not config.RETAIN_DATASET_FIRESTORE:
            retention_flag = False

        if retention_flag is True:
            logger.info("Retention flag is on. Process is skipped.")
            return None

        previous_dataset_version_from_firestore = (
            self._calculate_previous_dataset_version_from_firestore(
                current_dataset_survey_id, current_dataset_period_id
            )
        )

        if previous_dataset_version_from_firestore < 1:
            logger.info(
                "Previous create-dataset version deletion is not required. Process is skipped."
            )
            return None

        if previous_dataset_version_from_firestore != current_dataset_version - 1:
            logger.error(
                f"Previous create-dataset version calculated from firestore does not match."
                f" Expected version: '{current_dataset_version - 1}' Actual version in FireStore:"
                f" '{previous_dataset_version_from_firestore}'."
                f" New create-dataset may have not been saved properly. Process is skipped."
            )
            return None

        self.dataset_writer_service.try_perform_delete_previous_version_dataset_batch(
            current_dataset_survey_id,
            current_dataset_period_id,
            previous_dataset_version_from_firestore,
        )