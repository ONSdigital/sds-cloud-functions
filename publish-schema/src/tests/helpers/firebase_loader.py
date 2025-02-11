from config.config import CONFIG
from google.cloud import firestore


class FirebaseLoader:
    def __init__(self):
        self.client = self._connect_client()
        self.schemas_collection = self._set_collection("schemas")

    def _connect_client(self) -> firestore.Client:
        """
        Connect to the firestore client using PROJECT_ID
        """
        return firestore.Client(
            project=CONFIG.PROJECT_ID, database=CONFIG.FIRESTORE_DB_NAME
        )

    def _set_collection(self, collection) -> firestore.CollectionReference:
        """
        Setup the collection reference for schemas and datasets
        """
        return self.client.collection(collection)


firebase_loader = FirebaseLoader()
