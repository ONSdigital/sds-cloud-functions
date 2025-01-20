from config.config import config
from google.cloud import exceptions, storage


class BucketLoader:
    def __init__(self):
        self.dataset_bucket = self._initialise_bucket(config.DATASET_BUCKET_NAME)

    def get_schema_bucket(self) -> storage.Bucket:
        """
        Get the schema bucket from Google cloud
        """
        return self.schema_bucket

    def get_dataset_bucket(self) -> storage.Bucket:
        """
        Get the dataset bucket from Google cloud
        """
        return self.dataset_bucket

    def _initialise_bucket(self, bucket_name) -> storage.Bucket:
        """
        Connect to google cloud storage client using PROJECT_ID
        If bucket does not exists, then create the bucket
        Else connect to the bucket

        Parameters:
        bucket_name (str): The bucket name
        """
        if config.CONF == "unit":
            return None

        __storage_client = storage.Client(project=config.PROJECT_ID)
        try:
            bucket = __storage_client.get_bucket(
                bucket_name,
            )
        except exceptions.NotFound as exc:
            raise RuntimeError("Bucket is not found.") from exc

        return bucket


bucket_loader = BucketLoader()
