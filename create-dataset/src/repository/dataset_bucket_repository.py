from config.logging_config import logging
from models.dataset_models import RawDataset
from repositories.bucket_loader import bucket_loader
from repositories.bucket_repository import BucketRepository

logger = logging.getLogger(__name__)


class DatasetBucketRepository(BucketRepository):
    def __init__(self):
        self.bucket = bucket_loader.get_dataset_bucket()

    def get_dataset_file_as_json(self, filename: str) -> object:
        """
        Queries google bucket for file with a specific name and returns it as json.

        Parameters:
        filename (str): name of file being queried.

        Returns:
        object: raw create-dataset from the bucket file as json.
        """
        return self.get_bucket_file_as_json(filename)

    def fetch_oldest_filename_from_bucket(self) -> str | None:
        """
        Fetches the filename with the oldest 'last modified' date from the bucket.

        Returns:
        str: filename with the oldest 'last modified' date from the bucket.
        """
        blobs = self.bucket.list_blobs()
        oldest_blob = None
        oldest_date = None

        for blob in blobs:
            last_modified = blob.updated

            if oldest_date is None or last_modified < oldest_date:
                oldest_date = last_modified
                oldest_blob = blob

        return oldest_blob.name if oldest_blob else None
