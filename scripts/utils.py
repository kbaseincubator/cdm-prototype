from pathlib import Path


def get_genome_ids_with_lineage(
        taxonomy_files: list[str | Path],
        lineages: list[str]
) -> list[str]:
    """
    Get genome ids with the specified lineage from GTDB taxonomy files (e.g. bac120_taxonomy_r214.tsv).

    :param taxonomy_files: list of path of taxonomy files
    :param lineages: list of target lineages
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
