from config.config_factory import config
from firebase_admin import firestore
from logging_config import logging
from models.dataset_models import DatasetMetadata, DatasetMetadataWithoutId, UnitDataset
from services.shared.byte_conversion_service import ByteConversionService

logger = logging.getLogger(__name__)


class DatasetFirebaseRepository:
    MAX_BATCH_SIZE_BYTES = 9 * 1024 * 1024

    def __init__(self):
        # Initialize Firestore client
        self.client = firestore.Client(project=config.PROJECT_ID, database=config.DATABASE)
        # Initialize Firestore collections
        self.dataset_collection = self.client.collection("datasets")

    def get_latest_dataset_with_survey_id_and_period_id(
        self, survey_id: str, period_id: str
    ) -> DatasetMetadataWithoutId | None:
        """
        Gets the latest dataset metadata from firestore with a specific survey_id and period_id.

        Parameters:
        survey_id (str): survey_id of the specified dataset.
        period_id (str): period_id of the specified dataset.
        """
        latest_dataset = (
            self.dataset_collection
            .where("survey_id", "==", survey_id)
            .where("period_id", "==", period_id)
            .order_by("sds_dataset_version", direction=firestore.Query.DESCENDING)
            .limit(1)
            .stream()
        )

        dataset_metadata: DatasetMetadataWithoutId = None
        for dataset in latest_dataset:
            dataset_metadata: DatasetMetadataWithoutId = {**(dataset.to_dict())}

        return dataset_metadata

    def perform_batched_dataset_write(
        self,
        dataset_id: str,
        dataset_metadata_without_id: DatasetMetadataWithoutId,
        unit_data_collection_with_metadata: list[UnitDataset],
        extracted_unit_data_identifiers: list[str],
    ) -> None:
        """
        Write dataset metadata and unit data to firestore in batches.

        Parameters:
        dataset_id (str): The unique id of the dataset
        dataset_metadata_without_id (DatasetMetadataWithoutId): The metadata of the dataset without its id
        unit_data_collection_with_metadata (list[UnitDataset]): The collection of unit data associated with the new dataset
        extracted_unit_data_identifiers (list[str]): List of identifiers ordered to match the identifier for each set of
        unit data in the collection.

        """
        new_dataset_document = self.dataset_collection.document(dataset_id)
        unit_data_collection_snapshot = new_dataset_document.collection("units")

        try:
            batch = self.client.batch()
            batch.set(new_dataset_document, dataset_metadata_without_id, merge=True)
            batch.commit()

            batch = self.client.batch()
            batch_size_bytes = 0

            for (unit_data, unit_identifier) in zip(unit_data_collection_with_metadata, extracted_unit_data_identifiers):

                unit_data_size_bytes = ByteConversionService.get_serialized_size(unit_data)

                if batch_size_bytes + unit_data_size_bytes >= self.MAX_BATCH_SIZE_BYTES:
                    batch.commit()
                    batch = self.client.batch()
                    batch_size_bytes = 0

                new_unit = unit_data_collection_snapshot.document(unit_identifier)
                batch.set(new_unit, unit_data, merge=True)
                batch_size_bytes += unit_data_size_bytes

            if batch_size_bytes > 0:
                batch.commit()

        except Exception as exc:
            # If an error occurs during the batch write, the dataset and all its sub collections are deleted
            logger.error(f"Error performing batched dataset write: {exc}")
            logger.info("Performing clean up of dataset and sub collections")
            logger.debug(f"Deleting dataset with id: {dataset_id}")

            self.delete_dataset_with_dataset_id(dataset_id)

            logger.info("Dataset clean up is completed")

            raise RuntimeError("Error performing batched dataset write.") from exc

    def delete_dataset_with_dataset_id(self, dataset_id: str) -> None:
        """
        Deletes the dataset with the specified dataset id.

        Parameters:
        dataset_id (str): The unique id of the dataset
        """
        try:
            doc = self.dataset_collection.document(dataset_id).get()

            for sub_collection in doc.reference.collections():
                self.delete_sub_collection_in_batches(sub_collection)

            doc.reference.delete()

        except Exception as exc:
            logger.error(f"Error deleting dataset: {exc}")
            raise RuntimeError("Error deleting dataset.") from exc

    def delete_sub_collection_in_batches(
        self,
        sub_collection_ref: firestore.CollectionReference,
    ) -> None:
        """
        Deletes a sub collection in batches.

        Parameters:
        sub_collection_ref (firestore.CollectionReference): The reference to the sub collection
        """
        try:
            cursor = None
            limit = 10

            batch = self.client.batch()
            batch_size_bytes = 0

            while True:

                if cursor:
                    docs = list(sub_collection_ref
                                .limit(limit)
                                .order_by("__name__")
                                .start_after(cursor)
                                .stream())

                else:
                    docs = list(sub_collection_ref
                                .limit(limit)
                                .order_by("__name__")
                                .stream())

                if not docs:
                    break

                for doc in docs:
                    doc_size_bytes = ByteConversionService.get_serialized_size(doc.to_dict())

                    if batch_size_bytes + doc_size_bytes >= self.MAX_BATCH_SIZE_BYTES:
                        batch.commit()
                        batch = self.client.batch()
                        batch_size_bytes = 0

                    batch.delete(doc.reference)
                    batch_size_bytes += doc_size_bytes

                cursor = docs[-1]

            if batch_size_bytes > 0:
                batch.commit()

        except Exception as exc:
            logger.error(f"Error deleting sub collection in batches: {exc}")
            raise RuntimeError("Error deleting sub collection in batches.") from exc

    def get_unit_supplementary_data(
        self, dataset_id: str, identifier: str
    ) -> UnitDataset:
        """
        Get the unit supplementary data of a specified unit from a dataset collection

        Parameters:
        dataset_id (str): The unique id of the dataset
        identifier (str): The id of the unit on the dataset
        """
        return (
            self.dataset_collection.document(dataset_id)
            .collection("units")
            .document(identifier)
            .get()
            .to_dict()
        )

    def get_number_of_unit_supplementary_data_with_dataset_id(
        self, dataset_id: str, cursor=None
    ) -> int:
        """
        Get the number of unit supplementary data associated with a dataset id.
        This function use a cursor to create a snapshot of unit data and aggregate
        the count. This is to prevent 530 query timed out error when the number of
        unit data is too large.

        Parameters:
        dataset_id (str): The unique id of the dataset

        Returns:
        int: The number of unit supplementary data associated with the dataset id
        """
        limit = 1000
        count = 0

        collection_ref = self.dataset_collection.document(dataset_id).collection(
            "units"
        )

        while True:
            # Frees memory incurred in the recursion algorithm
            docs = []

            if cursor:
                docs = list(collection_ref
                            .limit(limit)
                            .order_by("__name__")
                            .start_after(cursor)
                            .stream())
            else:
                docs = list(collection_ref
                            .limit(limit)
                            .order_by("__name__")
                            .stream())

            count = count + len(docs)

            if len(docs) == limit:
                cursor = docs[limit - 1]
                continue

            break

        return count

    def get_dataset_metadata_collection(
        self, survey_id: str, period_id: str
    ) -> list[DatasetMetadata]:
        """
        Get the collection of dataset metadata from firestore associated with a specific survey and period id.

        Parameters:
        survey_id (str): The survey id of the dataset.
        period_id (str): The period id of the unit on the dataset.
        """
        returned_dataset_metadata = (
            self.dataset_collection
            .where("survey_id", "==", survey_id)
            .where("period_id", "==", period_id)
            .order_by("sds_dataset_version", direction=firestore.Query.DESCENDING)
            .stream()
        )

        dataset_metadata_list: list[DatasetMetadata] = []
        for dataset_metadata in returned_dataset_metadata:
            metadata: DatasetMetadata = {**dataset_metadata.to_dict()}
            metadata["dataset_id"] = dataset_metadata.id
            dataset_metadata_list.append(metadata)

        return dataset_metadata_list

    def get_dataset_metadata_with_survey_id_period_id_and_version(
        self, survey_id: str, period_id: str, version: int
    ) -> DatasetMetadata | None:
        """
        Get the dataset metadata from firestore associated with a specific survey and period id and version.

        Parameters:
        survey_id (str): The survey id of the dataset.
        period_id (str): The period id of the unit on the dataset.
        version (int): The version of the dataset.

        Returns:
        DatasetMetadata | None: The dataset metadata associated with the survey id, period id and version.
        """
        retrieved_dataset = (
            self.dataset_collection
            .where("survey_id", "==", survey_id)
            .where("period_id", "==", period_id)
            .where("sds_dataset_version", "==", version)
            .stream()
        )

        dataset_metadata: DatasetMetadata = None
        for dataset in retrieved_dataset:
            dataset_metadata: DatasetMetadata = {**(dataset.to_dict())}
            dataset_metadata["dataset_id"] = dataset.id

        return dataset_metadata
