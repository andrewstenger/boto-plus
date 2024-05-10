#!/usr/bin/env bash
# Get the storage size for multiple prefixes in a S3 bucket

BUCKET=<your-bucket-here>
BASE_URI=s3://$BUCKET

PREFIXES=("<first-prefix>" "<second-prefix>" "<third-prefix>")

for prefix in "${PREFIXES[@]}"
do
  FULL_PREFIX="${BASE_URI}/${prefix}/"
  printf "${FULL_PREFIX}"
  aws s3 ls --summarize --human-readable --recursive "${FULL_PREFIX}" | grep Size
  printf "\n"
done
