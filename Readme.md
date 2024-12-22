# GCP to AWS cloud transfer

This repo is for the python codes of the cloud function "bqtestdataintegeration"

To test any of the codes above use the bash command below

```bash
Step1:
gcloud auth list
Step2:
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" https://europe-west2-st-npr-ukg-pro-data-hub-8100.cloudfunctions.net/bqtestdataintegeration

## Note: You need to have editor access to do this




