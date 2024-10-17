from google.cloud import firestore

def get_dataset_metadata_collection() -> list[dict]:
        """
        """
        client = firestore.Client(
            project="ons-cir-sandbox-384314", database="ons-cir-sandbox-384314-sds"
        )

        datasets_collection = client.collection("datasets")

        returned_dataset_metadata = (
            datasets_collection.order_by("survey_id", direction=firestore.Query.DESCENDING)
            .stream()
        )

        dataset_metadata_list = []
        for dataset_metadata in returned_dataset_metadata:
            metadata = dataset_metadata.to_dict()
            dataset_metadata_list.append(metadata)

        return dataset_metadata_list