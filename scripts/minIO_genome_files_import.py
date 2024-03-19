import concurrent.futures
import os
from pathlib import Path

import boto3
from tqdm import tqdm

from scripts.utils import get_genome_ids_with_lineage, upload_to_s3

"""
This script uploads genome files from collections NCBI source directory to the specified S3 bucket.

Before running this script, make sure the following steps are completed:
1. Start the SSH tunnel to the minIO server at KBase Berkeley:
    ssh -L 9002:ci07:9002 $USERNAME@login1.berkeley.kbase.us -N
2. Get the secret key from the minIO server in Rancher and set the environment variable:
    i. Go to the minIO server in Rancher
        Stacks → CI → cdm-minio -> ci-core-cdm-minio-1 → Execute Shell → echo $MINIO_ROOT_PASSWORD
                                                      OR Command Tab -> Environment -> MINIO_ROOT_PASSWORD
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


def _generate_target_files_genome_source(
        source_dir: Path = SOURCE_DIR,
        suffix: str = 'protein.faa.gz'):
    """
    Generate target files for upload based on lineage genome IDs, source directory, and file suffix.

    :return
    target_files: list of tuples (upload_file_path, s3_key)
    no_match_genome_ids: list of genome IDs with no matching files
    multi_match_genome_ids: list of genome IDs with multiple matching files
    """
    meta_dir = Path('/global/homes/t/tgu/GTDB_meta')
    taxonomy_files = [meta_dir / 'bac120_taxonomy_r214.tsv', meta_dir / 'ar53_taxonomy_r214.tsv']
    lineages = ['c__Alphaproteobacteria']
    lineage_genome_ids = get_genome_ids_with_lineage(taxonomy_files, lineages)

    no_match_genome_ids = []
    multi_match_genome_ids = []
    target_files = []

    for genome_id in lineage_genome_ids:
        genome_dir = source_dir / genome_id
        matching_files = _find_files_with_suffix(genome_dir, suffix)

        if len(matching_files) == 1:
            upload_file = genome_dir / matching_files[0]
            target_files.append((upload_file, f'NCBI/{upload_file.name}'))
        else:
            multi_match_genome_ids.append(genome_id) if matching_files else no_match_genome_ids.append(genome_id)

    return target_files, no_match_genome_ids, multi_match_genome_ids


def main():

    target_files, no_match_genome_ids, multi_match_genome_ids = _generate_target_files_genome_source(SOURCE_DIR)

    s3 = boto3.client('s3',
                      endpoint_url=ENDPOINT_URL,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        progress_bar = tqdm(total=len(target_files))
        futures = []
        for upload_file, s3_key in target_files:
            futures.append(executor.submit(upload_to_s3,
                                           upload_file,
                                           s3_key,
                                           s3,
                                           BUCKET))

        for future in concurrent.futures.as_completed(futures):
            future.result()
            progress_bar.update(1)

    progress_bar.close()

    print("Summary:")
    print(f"Successfully uploaded: {len(target_files)}")
    print(f"Num of no matching files Genome: {len(no_match_genome_ids)}")
    print(f"{no_match_genome_ids[:10]}") if no_match_genome_ids else None
    print(f"Num of multiple matching files Genome: {len(multi_match_genome_ids)}")
    print(f"{multi_match_genome_ids[:10]}") if multi_match_genome_ids else None


if __name__ == '__main__':
    main()
