set -o xtrace
rm -v ./optout_master/*
aws s3 sync s3://aws-glue-us-east-1-fit-ds-live-raw-591853665740/optout_master/ ./optout_master/
set +o xtrace