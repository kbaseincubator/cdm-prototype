import os
from pathlib import Path

import boto3


def get_genome_ids_with_lineage(
        taxonomy_files: list[str | Path],
        lineages: list[str]
) -> list[str]:
    """
    Get genome ids with the specified lineage from GTDB taxonomy files (e.g. bac120_taxonomy_r214.tsv).

    :param taxonomy_files: list of path of taxonomy files
    :param lineages: list of target lineages

    :return: list of genome ids with the specified lineage
    """
    genome_ids = list()

    for file_path in taxonomy_files:
        with open(file_path, 'r') as file:
            for line in file:
                columns = line.strip().split('\t')

                for lineage in lineages:
                    if lineage in columns[1]:
                        # Trim the prefix of the genome id as it is not part of the id in NCBI
                        # e.g. RS_GCF_000979555.1 -> GCF_000979555.1
                        genome_ids.append(columns[0][3:])

    return genome_ids


def upload_to_s3(
        upload_file: Path,
        s3_key: str,
        s3: boto3.client,
        bucket: str):
    """
    Upload the specified file to the specified S3 bucket.

    :param upload_file: path of the file to upload
    :param s3_key: key of the file in the S3 bucket
    :param s3: boto3 client for S3
    :param bucket: name of the S3 bucket
    """
    try:
        # Skip uploading if the file already exists in the bucket
        s3.head_object(Bucket=bucket, Key=s3_key)
    except s3.exceptions.ClientError:
        s3.upload_file(str(upload_file), bucket, s3_key)


def find_files_with_suffix(
        directory: Path,
        suffix: str
) -> list[str]:
    """
    Find files with the specified suffix in the directory and its subdirectories.
    """
    matching_files = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(suffix):
                matching_files.append(os.path.join(root, file))

    return matching_files
