from firebase_admin import firestore


def perform_delete_on_collection_with_test_survey_id(
    client: firestore.Client,
    collection_ref: firestore.CollectionReference,
    test_survey_id: str,
) -> None:
    """
    Recursively deletes the collection and its subcollections.

    Parameters:
    collection_ref (firestore.CollectionReference): the reference of the collection being deleted.
    """

    # Query the collection for documents equivalent to survey_id LIKE "test_survey_id%"
    # \uf8ff is a unicode character that is greater than any other character
    doc_collection = (
        collection_ref.where("survey_id", ">=", test_survey_id)
        .where("survey_id", "<=", test_survey_id + "\uf8ff")
        .stream()
    )

    for doc in doc_collection:
        _delete_document(client, doc.reference)


def _delete_sub_collection_in_batches(
    client: firestore.Client,
    sub_collection_ref: firestore.CollectionReference,
    batch_size: int,
) -> int:
    """
    Deletes a sub collection in batches.

    Parameters:
    sub_collection_ref (firestore.CollectionReference): The reference to the sub collection
    batch_size (int): The size of the batch to be deleted

    Returns:
    int: Number of documents deleted in the sub collection.
    """
    docs = sub_collection_ref.limit(batch_size).get()
    doc_count = 0

    batch = client.batch()

    for doc in docs:
        doc_count += 1
        batch.delete(doc.reference)

    batch.commit()

    return doc_count


def _delete_document(
    client: firestore.Client, doc_ref: firestore.DocumentReference
) -> bool:
    """
    Deletes the dataset with Document Reference

    Parameters:
    doc_ref (firestore.DocumentReference): The reference to the dataset to be deleted.
    """
    batch_size = 100

    for sub_collection in doc_ref.collections():

        while True:

            doc_deleted = _delete_sub_collection_in_batches(
                client, sub_collection, batch_size
            )

            if doc_deleted < batch_size:
                break

    doc_ref.delete()

    return True
