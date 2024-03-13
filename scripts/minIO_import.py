import concurrent.futures
import os
from pathlib import Path

import boto3
from tqdm import tqdm

"""
This script uploads genome files from collections NCBI source directory to the specified S3 bucket.

Before running this script, make sure the following steps are completed:
1. Start the SSH tunnel to the minIO server at KBase Berkeley:
    ssh -L 9002:ci07:9002 $USERNAME@login1.berkeley.kbase.us -N
2. Get the secret key from the minIO server in Rancher and set the environment variable:
    i. Go to the minIO server in Rancher
        Stacks → CI → cdm-minio -> ci-core-cdm-minio-1 → Execute Shell → echo $MINIO_ROOT_PASSWORD
    ii. Set the environment variable in the terminal:
        export SECRET_KEY='your_secret_key'
"""

SECRET_KEY = os.environ.get('SECRET_KEY')
ACCESS_KEY = 'cdm-admin'
ENDPOINT_URL = 'http://localhost:9002'

SOURCE_DIR = Path('/global/cfs/cdirs/kbase/collections') / 'sourcedata' / 'NCBI' / 'NONE'
SUFFIX = 'protein.faa.gz'
BUCKET = 'cdm'
NUM_THREADS = 128


def _get_genome_ids_with_lineage(
        taxonomy_files: list[str | Path],
        lineages: list[str]
) -> list[str]:
    """
    Get genome ids with the specified lineage from GTDB taxonomy files (e.g. bac120_taxonomy_r214.tsv).
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


def _find_files_with_suffix(
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


def _upload_to_s3(
        genome_id: str,
        s3: boto3.client,
        source_dir: Path,
        suffix: str,
        bucket: str,
        no_match_genome_ids: list[str],
        multi_match_genome_ids: list[str]):
    """
    Upload the genome files to the specified S3 bucket.
    """
    genome_dir = source_dir / genome_id
    matching_files = _find_files_with_suffix(genome_dir, suffix)

    if len(matching_files) == 1:
        upload_file = genome_dir / matching_files[0]
        s3_key = 'NCBI/' + upload_file.name
        try:
            # skip uploading if the file already exists in the bucket
            s3.head_object(Bucket=bucket, Key=s3_key)
        except s3.exceptions.ClientError:
            s3.upload_file(upload_file, bucket, s3_key)
    else:
        multi_match_genome_ids.append(genome_id) if matching_files else no_match_genome_ids.append(genome_id)


def main():
    meta_dir = Path('/global/homes/t/tgu/GTDB_meta')
    taxonomy_files = [meta_dir / 'bac120_taxonomy_r214.tsv', meta_dir / 'ar53_taxonomy_r214.tsv']
    lineages = ['c__Alphaproteobacteria']
    lineage_genome_ids = _get_genome_ids_with_lineage(taxonomy_files, lineages)

    s3 = boto3.client('s3',
                      endpoint_url=ENDPOINT_URL,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    no_match_genome_ids = []
    multi_match_genome_ids = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        progress_bar = tqdm(total=len(lineage_genome_ids))
        futures = [executor.submit(_upload_to_s3,
                                   genome_id,
                                   s3,
                                   SOURCE_DIR,
                                   SUFFIX,
                                   BUCKET,
                                   multi_match_genome_ids,
                                   no_match_genome_ids) for genome_id in lineage_genome_ids]

        for future in concurrent.futures.as_completed(futures):
            future.result()
            progress_bar.update(1)

    progress_bar.close()

    print("Summary:")
    print(f"Successfully uploaded: {len(lineage_genome_ids) - len(no_match_genome_ids) - len(multi_match_genome_ids)}")
    print(f"Num of no matching files Genome: {len(no_match_genome_ids)}")
    print(f"{no_match_genome_ids[:10]}") if no_match_genome_ids else None
    print(f"Num of multiple matching files Genome: {len(multi_match_genome_ids)}")
    print(f"{multi_match_genome_ids[:10]}") if multi_match_genome_ids else None


if __name__ == '__main__':
    main()
