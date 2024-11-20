from config.config_factory import config
from config.logging_config import logging
from models.dataset_models import RawDataset
from repositories.dataset_bucket_repository import DatasetBucketRepository
from services.dataset_validator_service import DatasetValidatorService

logger = logging.getLogger(__name__)


class DatasetBucketService:
    def __init__(self):
        self.dataset_bucket_repository = DatasetBucketRepository()

    def get_and_validate_dataset(self, filename: str) -> RawDataset:
        """
        Validates and retrieves dataset-create from bucket
        Parameters:
        filename: name of file being retrieved from bucket
        """
        is_valid, message = DatasetValidatorService.validate_file_is_json(filename)
        if not is_valid:
            if config.AUTODELETE_DATASET_BUCKET_FILE is True:
                self.try_delete_bucket_file(filename)

            raise RuntimeError(message)

        raw_dataset = self.dataset_bucket_repository.get_dataset_file_as_json(filename)

        if config.AUTODELETE_DATASET_BUCKET_FILE is True:
            self.try_delete_bucket_file(filename)

        DatasetValidatorService.validate_raw_dataset(raw_dataset)

        return raw_dataset

    def try_delete_bucket_file(self, filename) -> None:
        """
        Tries to delete a file from the bucket, raises an error on failure.
        """
        try:
            self.dataset_bucket_repository.delete_bucket_file(filename)
        except Exception as exc:
            logger.debug(
                f"Failed to delete file {filename} from bucket {config.DATASET_BUCKET_NAME} with error: {exc}"
            )
            raise RuntimeError("Failed to delete file from dataset-create bucket.") from exc

    def try_fetch_oldest_filename_from_bucket(self) -> str | None:
        """
        Fetches the first filename from the bucket.
        Returns:
        str: filename from the bucket.
        """
        try:
            filename = (
                self.dataset_bucket_repository.fetch_oldest_filename_from_bucket()
            )

            return filename

        except Exception as exc:
            logger.debug(
                f"Failed to fetch first filename from bucket {config.DATASET_BUCKET_NAME} with error: {exc}"
            )
            raise RuntimeError("Failed to fetch first filename from dataset-create bucket.") from exc