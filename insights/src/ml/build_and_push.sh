#!/usr/bin/env bash

# This script shows how to build the Docker image and push it to ECR to be ready for use
# by SageMaker.

# The argument to this script is the image name. This will be used as the image on the local
# machine and combined with the account and region to form the repository name for ECR.

PREVIOUS_DIR=${PWD}
SCRIPT_DIR=$(dirname $0)
cd $SCRIPT_DIR

pkg_name=$1
pkg_version=$2
opt_registries=$3

if [[ ${pkg_name} == "" || ${pkg_version} == "" ]]
then
    echo "Usage: $0 <project_name> <pkg_version> <additional_registries>?"
    exit 1
fi

# Get the account number associated with the current IAM credentials
account="591853665740"

if [[ $? -ne 0 ]]
then
    exit 255
fi

# Get the region defined in the current configuration (default to us-west-2 if none defined)
region=$(aws configure get region)
region=${region:-us-east-1}

if [[ ${pkg_version} == *SNAPSHOT* ]];
then
    repo_name="${pkg_name}-snapshot"
else
    repo_name="${pkg_name}-release"
fi
echo "image name sett"

# Get the login command from ECR and execute it directly
# Login to account containing the prebuilt xgboost image in ECR

if [[ $? -ne 0 && ${opt_registries} == "" ]]
then
    $(aws ecr get-login --region ${region} --no-include-email)
else
    registry_ids="${opt_registries} ${account}"
    IFS=' ' read -ra registries_array <<< "$registry_ids"
    for registry_id in "${registries_array[@]}"
    do
        echo "Logging into $registry_id"
        $(aws ecr get-login --region ${region} --no-include-email --registry-ids ${registry_id})
    done
fi


# Build the docker image locally with the image name and then push it to ECR
# with the full name.
tag="${pkg_version}"
img_name="${repo_name}:${tag}"
echo "Building $img_name"
docker build -t ${img_name} .
ecr_repo_name="${account}.dkr.ecr.${region}.amazonaws.com/${img_name}"
echo " build command completed"
docker tag ${img_name} ${ecr_repo_name}
docker push ${ecr_repo_name}
echo " push completed"
echo " completed building image and pushed it"

cd "$PREVIOUS_DIR"




