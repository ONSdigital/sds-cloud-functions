import logging

from google.cloud import firestore

def fetch_dataset_guid_from_collection(firestore_client: firestore.Client) -> str:
    """
    Function that will fetch 1 dataset from the 'marked' collection in Firestore and return the guid of that dataset - if it exists.

    Parameters:
    project_id: The project ID of the project.
    database: The name of the Firestore database.
    """
    marked_datasets = firestore_client.collection("marked").where("deleted", "==", False).limit(1).get()

    if marked_datasets:
        dataset = marked_datasets[0].to_dict()
        return dataset.get('dataset_guid')
    return None

def fetch_document_for_deletion(dataset_guid: str, firestore_client: firestore.Client) -> firestore.DocumentReference:
    """
    Function that will fetch the document reference for the dataset to be deleted.

    Parameters:
    project_id: The project ID of the project.
    database: The name of the Firestore database.
    dataset_guid: The GUID of the dataset to be deleted.
    """
    doc_ref = firestore_client.collection("datasets").document(dataset_guid)
    return doc_ref

def mark_dataset_as_deleted(doc_ref: str, firestore_client: firestore.Client):
    """
    Function that will mark the dataset as deleted in Firestore.

    Parameters:
    doc_ref: The document reference of the dataset to be marked as deleted.
    project_id: The project ID of the project.
    """

    document_reference = firestore_client.collection("marked").where("dataset_guid", "==", doc_ref).limit(1).get()[0].reference

    document_reference.update({"deleted": True})


def delete_dataset_with_dataset_id(doc_ref: firestore.DocumentReference, client: firestore.Client) -> None:
        """
        Deletes the dataset with the specified dataset id.

        Parameters:
        dataset_id (str): The unique id of the dataset
        """
        try:
            for sub_collection in doc_ref.collections():
                delete_sub_collection_in_batches(sub_collection, client)

            doc.reference.delete()

        except Exception as e:
            raise RuntimeError("Error deleting dataset.")

def delete_sub_collection_in_batches(
    self,
    sub_collection_ref: firestore.CollectionReference,
    client: firestore.Client,
    batch_size: int = 100
) -> None:
    """
    Deletes a sub collection in batches.

    Parameters:
    sub_collection_ref (firestore.CollectionReference): The reference to the sub collection
    """
    try:
        docs = sub_collection_ref.limit(batch_size).get()
        doc_count = 0

        batch = client.batch()

        for doc in docs:
            doc_count += 1
            batch.delete(doc.reference)

        batch.commit()

        if doc_count < batch_size:
            return None

        return delete_sub_collection_in_batches(sub_collection_ref, client)

    except Exception as e:
        raise RuntimeError("Error deleting sub collection in batches.")


"""def delete_document_and_subcollections(doc_ref: firestore.DocumentReference):
    
    for subcollections in doc_ref.collections():
        delete_collection_in_batches(subcollections, batch_size=100)
    
    doc_ref.delete()

def delete_collection_in_batches(collection_ref: firestore.CollectionReference, batch_size: int): 
    docs = collection_ref.limit(batch_size).get()
    doc_count = 0

    for doc in docs:
        doc_count += 1
        delete_document_and_subcollections(doc.reference)

    if doc_count < batch_size:
        return None

    return delete_collection_in_batches(collection_ref, batch_size)"""

if __name__ == "__main__":
    
    project_id = "ons-sds-jb"
    database_name = "ons-sds-jb-sds"
    firestore_client = firestore.Client(project=project_id, database=database_name)

    doc_guid = fetch_dataset_guid_from_collection(firestore_client)
    if doc_guid is not None:
        doc = fetch_document_for_deletion(doc_guid, firestore_client)
        delete_dataset_with_dataset_id(doc, firestore_client)

        logging.info(f"Dataset GUID: {doc_guid} has been deleted")

        mark_dataset_as_deleted(doc_guid, firestore_client)
        logging.info(f"Dataset GUID: {doc_guid} has been marked as deleted")
    else:
        logging.info("No datasets to delete")
        print("No datasets to delete")