import time

from google.cloud import firestore
from logging_config import logging
from config import config
from status import Status
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class DatasetDeleter:
    """
    Class that will handle the deletion of a dataset in Firestore.
    DatasetDeleter will only pick 1 dataset with Pending deletion status and
    process the deletion of that dataset.
    """
    def __init__(self):
        # Mark the start time of the deletion process
        self.start_time = time.time()

        # Initialize Firestore client
        self.client = firestore.Client(project=config.PROJECT_ID, database=config.DATABASE)

        # Initialize Firestore collections
        self.mark_deletion_collection = self.client.collection("marked_for_deletion")
        self.dataset_collection = self.client.collection("datasets")

        # Dataset GUID for deletion
        self.guid = None
        # Record ID in the 'marked_for_deletion' collection
        self.marked_id = None


    def fetch_dataset_deletion_from_collection(self) -> None:
        """
        Function that will fetch 1 dataset with Processing or 
        Pending status from the 'marked_for_deletion' collection 
        in Firestore and store the guid of that dataset and the
        deletion record id.
        """
        # Fetch the dataset with status 'Processing' first
        marked_datasets = (
            self.mark_deletion_collection
            .where("status", "==", Status.PROCESSING)
            .limit(1)
            .stream()
        )

        for marked_dataset in marked_datasets:
            dataset = marked_dataset.to_dict()
            self.guid = dataset.get('dataset_guid')
            self.marked_id = marked_dataset.id

        if self.guid is not None:
            logger.info(
                f"Picking up last deletion process."
            )
            return

        # If no dataset with status 'Processing' is found, fetch the dataset with status 'Pending'
        marked_datasets = (
            self.mark_deletion_collection
            .where("status", "==", Status.PENDING)
            .limit(1)
            .stream()
        )

        for marked_dataset in marked_datasets:
            dataset = marked_dataset.to_dict()
            self.guid = dataset.get('dataset_guid')
            self.marked_id = marked_dataset.id
        

    def fetch_dataset_with_guid(self) -> firestore.DocumentReference | None:
        """
        Function that will fetch the document reference for the dataset to be deleted using guid.

        Returns:
        doc_ref: The document reference of the dataset to be deleted.
        """

        return self.dataset_collection.document(self.guid)


    def delete_dataset_with_dataset_id(self, doc_ref: firestore.DocumentReference) -> bool:
            """
            Deletes the dataset with the specified dataset id.

            Parameters:
            dataset_id (str): The unique id of the dataset

            Returns:
            bool: False if the deletion process is ended due to timeout, True otherwise.
            """
            try:
                batch_size = config.DELETION_BATCH_SIZE

                for sub_collection in doc_ref.collections():
                    
                    while True:

                        doc_deleted = self.delete_sub_collection_in_batches(sub_collection)

                        # Check if the dataset deletion process has reached the timeout
                        # If it has, exit the deletion process immediately
                        if self.is_dataset_deletion_timeout():
                            return False

                        if doc_deleted < batch_size:
                            break

                doc_ref.delete()

                return True

            except Exception as e:
                raise RuntimeError("Error deleting dataset.")
            

    def delete_sub_collection_in_batches(
        self,
        sub_collection_ref: firestore.CollectionReference,
    ) -> int:
        """
        Deletes a sub collection in batches.

        Parameters:
        sub_collection_ref (firestore.CollectionReference): The reference to the sub collection

        Returns:
        bool: False if the deletion process is ended due to timeout, True otherwise.
        """
        try:
            batch_size = config.DELETION_BATCH_SIZE

            docs = sub_collection_ref.limit(batch_size).get()
            doc_count = 0

            batch = self.client.batch()

            for doc in docs:
                doc_count += 1
                batch.delete(doc.reference)                    

            batch.commit()

            return doc_count

        except Exception as e:
            raise RuntimeError("Error deleting sub collection in batches.")
        
    
    def mark_dataset_as_deleted(self):
        """
        Function that will mark the dataset as deleted in Firestore.
        """
        self.mark_dataset_deletion_status(Status.DELETED)


    def mark_dataset_as_error(self):
        """
        Function that will mark the dataset as error in Firestore.
        """
        self.mark_dataset_deletion_status(Status.ERROR)


    def mark_dataset_as_processing(self):
        """
        Function that will mark the dataset as processing in Firestore.
        """
        self.mark_dataset_deletion_status(Status.PROCESSING)


    def mark_dataset_deletion_status(self, dataset_status: str):
        """
        Function that will mark the dataset deletion status in Firestore.

        Parameters:
        dataset_guid: The document guid of the dataset to be marked as deleted.
        status: The status of the deletion.
        """
        try:
            document_reference = self.mark_deletion_collection.document(self.marked_id)

            document_reference.update({"status": dataset_status})

            if dataset_status == Status.DELETED:
                # Mark the deleted timestamp
                timestamp = self.get_current_time_with_format()
                document_reference.update({"deleted_at": timestamp})

            logger.info(
                f"Deletion record has been marked as '{dataset_status}'."
            )
        except Exception as e:
            raise RuntimeError("Error marking status on deletion record.")

    
    def is_document_exists(self, doc_ref: firestore.DocumentReference) -> bool:
        """
        Function that will check if the document exists in Firestore.

        Parameters:
        doc_ref: The document reference of the dataset to be checked.
        """
        return doc_ref.get().exists
    
    
    def is_dataset_deletion_timeout(self) -> bool:
        """
        Function that will check if the dataset deletion process has reached the timeout.

        Returns:
        bool: True if the dataset deletion process has reached the timeout, False otherwise.
        """
        return time.time() - self.start_time > config.PROCESS_TIMEOUT
    
    
    def get_current_time_with_format(self) -> str:
        """
        Function that will return the current time in UTC with the specified format.

        Returns:
        str: The current time in UTC with the specified format.
        """
        utc_dt = datetime.now(timezone.utc) # UTC time
        dt = utc_dt.astimezone() # local time
        timestamp = dt.strftime(config.TIME_FORMAT)

        return timestamp    