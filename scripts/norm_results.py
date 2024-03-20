"""
This script is used to normalize the results of different tools used for the CDM project.

"""
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd

from scripts.utils import find_files_with_suffix

COLL_ROOT = Path('/global/cfs/cdirs/kbase/collections')
FASTANI_RESULTS_DIR = Path(
    '/global/cfs/cdirs/kbase/ke_prototype/mcashman/analysis/pairwise_ANI/species_level_runs/RESULTS/MERGED_OUT')
CLADE_ID_FILE = Path(
    '/global/cfs/cdirs/kbase/ke_prototype/mcashman/CDM/sample_clades/species_clade_list_filtered_f__Rhodanobacteraceae.txt')


def normalize_eggnog_results(
        coll_root: Path = COLL_ROOT,
        load_ver: str = 'f__Rhodanobacteraceae'
):
    """
    eggNOG is executed with the collections framework, so the results are in the collections directory.

    This function process the emapper.annotations.xlsx file from eggNOG and append the genome_id to the first column.
    And save the processed file to the same directory with the prefix "processed_".

    :param coll_root: root directory of the collections project
    :param load_ver: load version of the eggNOG results

    :return: list of processed eggNOG result files
    """

    data_dir = coll_root / 'collectionsdata' / 'NONE' / 'CDM' / load_ver / 'eggnog'
    batch_dirs = [x for x in data_dir.iterdir() if x.is_dir()]

    processed_files = list()
    for batch_dir in batch_dirs:
        genome_dirs = [x for x in batch_dir.iterdir() if x.is_dir()]

        for genome_dir in genome_dirs:
            anno_result = Path(find_files_with_suffix(genome_dir, 'emapper.annotations.xlsx')[0])

            # Read the file into a DataFrame, skipping initial rows starting with ##
            df = pd.read_excel(anno_result, comment='#')
            df = df.dropna(how='all')
            df.insert(0, 'genome_id', genome_dir.name)

            # Save the processed DataFrame as CSV to the same directory with the prefix "processed_"
            output_file_name = "processed_" + anno_result.name.replace(".xlsx", ".csv")
            output_file_path = anno_result.parent / output_file_name
            df.to_csv(output_file_path, index=False)
            processed_files.append(output_file_path)

    print(f'Processed {len(processed_files)} genome eggNOG results')

    return processed_files


def normalize_fastani_results(
        fastani_result_dir: Path = FASTANI_RESULTS_DIR,
        clade_id_file: Path = CLADE_ID_FILE
):
    """
    Normalize the FastANI result file (.txt result) by processing each fastani result and concatenate them into a
    single DataFrame.

    The processed DataFrame is saved to the current working directory as "processed_fastani_results.csv".

    :param fastani_result_dir: directory of the FastANI result files
    :param clade_id_file: file that contains clade names for genomes belongs to Rhodanobacteraceae
    """
    with open(clade_id_file, 'r') as file:
        clade_ids = [line.strip() for line in file]

    fast_ani_results = os.listdir(fastani_result_dir)

    clade_target_files, dfs = defaultdict(list), list()
    for clade_id in clade_ids:
        for result in fast_ani_results:
            if clade_id in result and result.endswith('.txt'):
                clade_target_files[clade_id].append(fastani_result_dir / result)

                df = pd.read_csv(
                    fastani_result_dir / result,
                    sep="\s+",  # whitespace separated
                    header=None,  # no header
                    names=["G1_path", "G2_path", "ANI", "Overlap", "Total"]
                )

                df["G1"] = df["G1_path"].apply(lambda x: '_'.join(Path(x).name.split("_")[:2]))
                df["G2"] = df["G2_path"].apply(lambda x: '_'.join(Path(x).name.split("_")[:2]))

                df = df[['G1', 'G2', 'ANI', 'Overlap', 'Total']]

                dfs.append(df)

    master_df = pd.concat(dfs, ignore_index=True)

    master_df.to_csv('processed_fastani_results.csv', index=False)


def main():
    normalize_eggnog_results()
    normalize_fastani_results()


if __name__ == '__main__':
    main()
