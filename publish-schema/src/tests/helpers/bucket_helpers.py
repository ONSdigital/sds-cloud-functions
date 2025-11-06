def delete_blobs_with_test_survey_id(bucket, test_survey_id: str) -> None:
    """
    Method to delete all blobs related to the test survey id in the specified bucket.

    Parameters:
        bucket: the bucket to clean
        test_survey_id: the test survey id

    Returns:
        None
    """
    blobs = bucket.list_blobs(prefix=test_survey_id)

    for blob in blobs:
        blob.delete()
