```
PROJECT_NAME=ons-cir-sandbox-384314
gcloud auth login
gcloud config set project $PROJECT_NAME

gcloud functions deploy sync-firestore-metrics \
--no-allow-unauthenticated \
--gen2 \
--ingress-settings=all \
--runtime=python311 \
--region=europe-west2 \
--source=. \
--entry-point=sync_firestore \
--timeout=3600s \
--memory=512MiB \
--cpu=1 \
--trigger-http
```
