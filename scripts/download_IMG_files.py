"""
This script downloads IMG files from the specified S3 bucket to the local directory in NERSC.
"""

import os
from collections import defaultdict
from pathlib import Path

import boto3

SECRET_KEY = os.environ.get('SECRET_KEY')
ACCESS_KEY = 'cdm-admin'
# Tunnel to the MinIO server - `ssh -f -N -L localhost:49002:ci07:9002 <kbase_dev_username>@login1.berkeley.kbase.us`
ENDPOINT_URL = 'http://localhost:49002/'
BUCKET = 'dts-staging'

COLL_ROOT = Path('/global/cfs/cdirs/kbase/collections')
SOURCE_DIR = COLL_ROOT / 'collectionssource' / 'NONE' / 'CDM' / 'IMG'

FILE_SUFFIXES = ['.faa']  # ['.fna', '.gff', '.faa'] - fna and gff files are also available in the bucket


def main():
    s3_client = boto3.client('s3',
                             endpoint_url=ENDPOINT_URL,
                             aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY)

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET)

    genome_count = defaultdict(int)
    total_files_downloaded = 0

    for page in page_iterator:
        objs = page.get('Contents', [])
        for obj in objs:
            key = obj['Key']

            # Filter to download files with specified suffixes
            if any(key.lower().endswith(suffix.lower()) for suffix in FILE_SUFFIXES):
                parts = key.split('/')
                # Assuming the structure 'bucket_name/uuid/img/submissions/genome_folder/file_name' - genome_folder is an integer
                genome_folder = parts[-2]
                file_name = parts[-1]

                genome_directory = SOURCE_DIR / genome_folder
                os.makedirs(genome_directory, exist_ok=True)

                local_file_path = genome_directory / file_name

                # Download the file
                s3_client.download_file(BUCKET, key, local_file_path)
                print(f"Downloaded {file_name} to {local_file_path}")

                genome_count[genome_folder] += 1
                total_files_downloaded += 1

    print(f"Total files downloaded: {total_files_downloaded}")
    print(f"Genome count: {len(genome_count)}")


if __name__ == '__main__':
    main()
