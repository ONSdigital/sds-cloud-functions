# sds-cloud-functions

Repository for Google Cloud Functions used with SDS


## dataset_creation cloud Function


## dataset_deletion cloud Function

`dataset-deletion` runs as a Cloud Function. It is triggered by cloud scheduler to periodically deleting dataset from SDS database when marked for deletion
To deploy the Cloud Function on a personal sandbox:

* Make sure to setup the sandbox project using the latest IAC
* Create a PR on this repository, make a change under `delete-datasets` to trigger the cloud build
