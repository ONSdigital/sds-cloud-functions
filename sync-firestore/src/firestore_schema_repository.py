from google.cloud import firestore

def get_schema_metadata_collection() -> list[dict]:
        """
        """
        client = firestore.Client(
            project="ons-cir-sandbox-384314", database="ons-cir-sandbox-384314-sds"
        )

        schemas_collection = client.collection("schemas")

        returned_schema_metadata = (
            schemas_collection.order_by("survey_id", direction=firestore.Query.DESCENDING)
            .stream()
        )

        schema_metadata_list = []
        for schema_metadata in returned_schema_metadata:
            metadata = schema_metadata.to_dict()
            schema_metadata_list.append(metadata)

        return schema_metadata_list