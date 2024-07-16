## Performance of EggNOG-mapper

### Test with user (project) drive

Conducted performance tests at NERSC utilizing the Taskfarmer scheduler to process 50 IMG .faa genome files.
The results are summarized in the table below:

| Number of nodes | Parallelization within a node | Time to finish 50 genomes | Average node time per genome |
|-----------------|-------------------------------|---------------------------|------------------------------|
| 1               | 1                             | N.A. (20 finished)        | 9 min                        |
| 1               | 2                             | N.A. (16 finished)        | 11.25 min                    |
| 1               | 5                             | N.A. (20 finished)        | 9 min                        |
| 1               | 10                            | N.A. (17 finished)        | 10.5 min                     |
| 2               | 1                             | N.A. (23 finished)        | 15.5 min                     |
| 5               | 1                             | N.A. (43 finished)        | 21 min                       |
| 10              | 1                             | N.A. (39 finished)        | 42 min                       |

(Each node has 3 hrs of wall time.)

### Test with scratch drive (Recommended by NERSC)

Copied the source files (same genome files as the above test) to `$SCRATCH` and ran the same tests. The results are
summarized in the table below:

| Number of nodes | Parallelization within a node | Time to finish 50 genomes                              | Total node time | Average node time per genome |
|-----------------|-------------------------------|--------------------------------------------------------|-----------------|------------------------------|
| 1               | 1                             | 54.83 mins                                             | 54.83 mins      | 1.1 min                      |
| 1               | 2                             | 39.77 mins (both batches)                              | 39.77 mins      | 0.79 min                     |
| 1               | 5                             | 27.92 mins (fastest batch), 37.11 mins (slowest batch) | 37.11 mins      | 0.74 min                     |
| 1               | 10                            | 22.62 mins (fastest batch), 34.7 mins (slowest batch)  | 34.7 mins       | 0.69 min                     |
| 1               | 15                            | 18.8 mins (fastest batch), 30.79 mins (slowest batch)  | 30.79 mins      | 0.62 min                     |
| 2               | 1                             | 25.91 mins (fastest node), 26.85 mins (slowest node)   | 52.76 mins      | 1.06 min                     |
| 5               | 1                             | 10.03 mins (fastest node), 13.24 mins (slowest node)   | 60.46 mins      | 1.21 min                     |

Note: Creating a symbolic link to the source files/tool libraries in the scratch directory does not seem to work,
as it defeats the purpose of using the scratch area for fast file access.
Instead, you need to use the `cp` command to copy the files to the scratch directory.

```commandline
shifter_realpath: failed to lstat /var/udiMount/cfs
FAILED to find real path for volume "from": /pscratch/sd/t/tgu/libraries/eggnog/5.0.2
FAILED to setup user-requested mounts.
FAILED to setup image.
```

### Notes from NERSC office hours

* All nodes across the system are identical, each equipped with 512GB of memory.