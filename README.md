# sds-cloud-functions

Repository for Google Cloud Functions used with SDS

## dataset-creation Cloud Function

`dataset_create` runs as a Cloud Function. It is Triggered by a cloud scheduler hourly during office hours.
To deploy the Cloud Function on a personal sandbox:

- Make sure to setup the sandbox project using the latest IAC
- Create a PR on this repository, make a change under `create-dataset` to trigger the cloud build

## dataset-deletion Cloud Function

`dataset-deletion` runs as a Cloud Function. It is triggered by cloud scheduler to periodically deleting dataset from SDS database when marked for deletion
To deploy the Cloud Function on a personal sandbox:

- Make sure to setup the sandbox project using the latest IAC
- Create a PR on this repository, make a change under `delete-datasets` to trigger the cloud build

## publish-schema Cloud Function

`publish-schema` runs as a Cloud Function. It is triggered by PubSub to publish schema to the SDS database.

- Make sure to setup the sandbox project using the latest IAC
- Create a PR on this repository, make a change under `publish-schema` to trigger the cloud build
