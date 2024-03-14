import concurrent.futures
import os
from pathlib import Path

from scripts.utils import get_genome_ids_with_lineage

"""
This script creates a custom collection source version for eggNOG CDM data. 

It creates a soft link from the NCBI source data directory to the CDM collection source directory for each genome id 
with the specified lineage.
"""

# update the following variables if collection, source version, and target extensions need to be changed
COLLECTION = 'CDM'
SOURCE_VER = 'eggNOG'
TARGET_EXT = ['protein.faa.gz']  # available extensions: 'genomic.fna.gz', 'genomic.gbff.gz', 'protein.faa.gz'
NUM_THREADS = 128


def _create_softlink_between_dirs(
        new_dir: Path,
        target_dir: Path,
        extensions: list[str]
) -> Path | None:
    """
    Establish a symbolic link from new_dir to target_dir, contingent on the presence of files matching the specified extensions.

    return: None if the link is created successfully, otherwise the target_dir.
    """

    files = os.listdir(target_dir)
    # Check if all extensions have at least one corresponding file in the target directory
    found_files = all(any(file.endswith(extension) for file in files) for extension in extensions)

    if not found_files:
        return target_dir

    if os.path.exists(new_dir) and os.path.islink(new_dir) and os.readlink(new_dir) == str(target_dir):
        return None

    try:
        os.symlink(target_dir, new_dir, target_is_directory=True)
        return None
    except FileExistsError:
        raise ValueError(f"{new_dir} already exists and does not link to {target_dir} as expected")


def main():
    meta_dir = Path('/global/homes/t/tgu/GTDB_meta')
    taxonomy_files = [meta_dir / 'bac120_taxonomy_r214.tsv', meta_dir / 'ar53_taxonomy_r214.tsv']
    lineages = ['c__Alphaproteobacteria']
    lineage_genome_ids = get_genome_ids_with_lineage(taxonomy_files, lineages)

    root = Path('/global/cfs/cdirs/kbase/collections')
    cdm_coll_src_dir = root / 'collectionssource' / 'NONE' / COLLECTION / SOURCE_VER
    cdm_coll_src_dir.mkdir(parents=True, exist_ok=True)
    ncbi_source_dir = root / 'sourcedata' / 'NCBI' / 'NONE'

    skipped_dirs, failed_results = list(), list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(_create_softlink_between_dirs,
                                   cdm_coll_src_dir / genome_id,
                                   ncbi_source_dir / genome_id,
                                   TARGET_EXT) for genome_id in lineage_genome_ids]

        for future in concurrent.futures.as_completed(futures):
            try:
                skipped_dir = future.result()
                skipped_dirs.append(skipped_dir) if skipped_dir else None
            except Exception as e:
                failed_results.append((future, f"Error: {e}"))
                print(f"Error processing {future}: {e}")

    num_successes = len(lineage_genome_ids) - len(skipped_dirs) - len(failed_results)
    num_skipped = len(skipped_dirs)
    num_failures = len(failed_results)

    print(f"\nSummary:")
    print(f"Successfully processed {num_successes} directories.")
    print(f"Skipped {num_skipped} directories.")
    print(f"Failed to process {num_failures} directories.")


if __name__ == '__main__':
    main()
