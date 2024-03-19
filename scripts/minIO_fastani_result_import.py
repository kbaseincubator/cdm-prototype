import concurrent.futures
import os
from collections import defaultdict
from pathlib import Path

import boto3
from tqdm import tqdm

from scripts.utils import upload_to_s3

"""
This script uploads FastAPI result files from collections NCBI source directory to the specified S3 bucket.
"""

SECRET_KEY = os.environ.get('SECRET_KEY')
ACCESS_KEY = 'cdm-admin'
ENDPOINT_URL = 'http://localhost:9002'
BUCKET = 'cdm'

NUM_THREADS = 128

FASTANI_RESULTS_DIR = Path(
    '/global/cfs/cdirs/kbase/ke_prototype/mcashman/analysis/pairwise_ANI/species_level_runs/RESULTS/MERGED_OUT')
# file that contains clade names for genomes belongs to Rhodanobacteraceae (Mikaela provided)
# those clade names are prefixes for FastANI result files
CLADE_ID_FILE = Path(
    '/global/cfs/cdirs/kbase/ke_prototype/mcashman/CDM/sample_clades/species_clade_list_filtered_f__Rhodanobacteraceae.txt')


def _generate_target_files_fastani_results(
        fastani_result_dir: Path = FASTANI_RESULTS_DIR,
        clade_id_file: Path = CLADE_ID_FILE
):
    with open(clade_id_file, 'r') as file:
        clade_ids = [line.strip() for line in file]

    fast_ani_results = os.listdir(fastani_result_dir)

    clade_target_files = defaultdict(list)
    for clade_id in clade_ids:
        for result in fast_ani_results:
            if clade_id in result and result.endswith(('.txt', '.txt.matrix')):
                clade_target_files[clade_id].append(fastani_result_dir / result)

    target_files = []
    for sublist in clade_target_files.values():
        for file in sublist:
            target_files.append((file, f'FastANI/Rhodanobacteraceae/{file.name}'))

    return target_files


def main():
    target_files = _generate_target_files_fastani_results()

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


if __name__ == '__main__':
    main()
