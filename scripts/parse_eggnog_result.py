"""
This script parses the Eggnog result files and uploads the processed files to the MinIO server.
"""
import json
import os
from pathlib import Path

import boto3
import pandas as pd

SECRET_KEY = os.environ.get('SECRET_KEY')
ACCESS_KEY = 'cdm-admin'
# Tunnel to the MinIO server - `ssh -f -N -L localhost:49002:ci07:9002 <kbase_dev_username>@login1.berkeley.kbase.us`
ENDPOINT_URL = 'http://localhost:49002/'
BUCKET = 'cdm-lake'

COLL_ROOT = Path('/global/cfs/cdirs/kbase/collections')
kbase_collection = 'CDM'
load_ver = 'IMG'
RESULT_DIR = COLL_ROOT / 'collectionsdata' / 'NONE' / kbase_collection / load_ver / 'eggnog'
COMPUTE_OUTPUT_PREFIX = 'job'


def _get_batch_dirs(result_dir: Path) -> list[str]:
    """Get the list of directories for batches"""

    batch_dirs = [d for d in os.listdir(result_dir)
                  if os.path.isdir(os.path.join(result_dir, d))
                  and d.startswith(COMPUTE_OUTPUT_PREFIX)]

    return batch_dirs


def _upload_to_minio(processed_file: Path, data_id: str):
    """Upload file to MinIO."""

    s3_path = f'IMG-source/eggnog_results/{data_id}/{processed_file.name}'
    s3_client = boto3.client('s3',
                             endpoint_url=ENDPOINT_URL,
                             aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY)
    try:
        s3_client.upload_file(str(processed_file), BUCKET, s3_path)
        print(f"File has been uploaded to s3://{BUCKET}/{s3_path}")
    except Exception as e:
        raise ValueError(f"Error uploading {processed_file} to MinIO: {e}")


def _validate_directory(data_dir: Path) -> str:
    """Validate if the necessary files are present in the directory."""

    # The presence of the metadata file indicates the eggnog run completed successfully
    metadata_file = data_dir / 'eggnog_run_metadata.json'
    if not metadata_file.exists():
        raise FileNotFoundError(f'eggnog_run_metadata.json not found in {data_dir}')

    with open(metadata_file, 'r') as file:
        metadata = json.load(file)
        source_file = metadata.get('source_file')
        if not source_file:
            raise ValueError('source_file not found in metadata')

    return source_file


def _read_and_process_data(data_dir: Path, source_file: str) -> pd.DataFrame:
    """Read and process annotation data."""

    # read result annotation xlsx file
    anno_file = f'{Path(source_file).name}.emapper.annotations.xlsx'
    if not (data_dir / anno_file).exists():
        raise FileNotFoundError(f'{anno_file} not found in {data_dir}')

    df = pd.read_excel(data_dir / anno_file, header=None)

    # Skip the first 2 rows and the last 3 rows, they are just metadata added by the eggnog tool and not useful
    df = df.iloc[2:-3].reset_index(drop=True)
    # Set the first row as the column names
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)

    return df


def _save_processed_data(data_dir: Path,
                         df: pd.DataFrame,
                         data_id: str,
                         source_file: str) -> Path:
    """Add metadata (IMG submission ID and source file name) to the dataframe and save the processed data."""

    # Add the IMG submission ID and file name to the dataframe for future reference (foreign key)
    # Add more metadata if needed
    df['img_submission_id'] = data_id
    df['source_file_name'] = Path(source_file).name

    processed_file = data_dir / f'{Path(source_file).name}.emapper.annotations.processed.csv'
    df.to_csv(processed_file, index=False)

    return processed_file


def _process_annotation_file(data_dir: Path, data_id: str):
    """Process the annotation file and upload the processed file to MinIO."""

    try:
        source_file = _validate_directory(data_dir)
        df = _read_and_process_data(data_dir, source_file)
        processed_file = _save_processed_data(data_dir, df, data_id, source_file)
        _upload_to_minio(processed_file, data_id)
        return True
    except (FileNotFoundError, ValueError) as e:
        print(f"Error processing files in {data_dir}: {e}")
    except Exception as e:
        print(f"Unexpected error processing files in {data_dir}: {e}")
    return False


def main():
    batch_dirs = _get_batch_dirs(RESULT_DIR)
    total_data_ids, processed_data_ids = 0, 0
    for batch_dir in batch_dirs:
        data_ids = [item for item in os.listdir(os.path.join(RESULT_DIR, batch_dir)) if
                    os.path.isdir(os.path.join(RESULT_DIR, batch_dir, item))]
        total_data_ids += len(data_ids)
        for data_id in data_ids:
            data_dir = RESULT_DIR / batch_dir / data_id
            if _process_annotation_file(data_dir, data_id):
                processed_data_ids += 1

    print(f"Total data IDs: {total_data_ids}")
    print(f"Processed data IDs: {processed_data_ids}")


if __name__ == '__main__':
    main()
