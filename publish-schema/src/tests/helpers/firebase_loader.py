from google.cloud import firestore
from src.config.schema_config import CONFIG


class FirebaseLoader:
    def __init__(self):
        self.client = self._connect_client()
        self.schemas_collection = self._set_collection("schemas")

    def get_client(self) -> firestore.Client:
        """
        Get the firestore client
        """
        return self.client

    def get_schemas_collection(self) -> firestore.CollectionReference:
        """
        Get the schemas collection from firestore
        """
        return self.schemas_collection

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
