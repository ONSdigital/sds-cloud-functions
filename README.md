# sds-cloud-functions

Repository for Google Cloud Functions used with SDS

## dataset_deletion cloud Function

`dataset-deletion` runs as a Cloud Function. It is triggered by cloud scheduler to periodically deleting dataset from SDS database when marked for deletion
To deploy the Cloud Function, run the following locally, but set the PROJECT_NAME environment variables first:

```bash
PROJECT_NAME=ons-sds-jb
gcloud auth login
gcloud config set project $PROJECT_NAME

cd delete-datasets/src
gcloud functions deploy dataset-deletion-function \
--no-allow-unauthenticated \
--gen2 \
--ingress-settings=all \
--runtime=python311 \
--region=europe-west2 \
--source=. \
--entry-point=delete_dataset \
--timeout=3600s \
--memory=10G \
--cpu=4 \
--trigger-http \
--set-env-vars="PROJECT_ID=$PROJECT_NAME,DATABASE=$PROJECT_NAME-sds,LOG_LEVEL=DEBUG,PROCESS_TIMEOUT=3400,DELETION_BATCH_SIZE=100"
```
