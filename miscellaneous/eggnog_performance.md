
## Performance of EggNOG-mapper

Conducted performance tests at NERSC utilizing the Taskfarmer scheduler to process 50 IMG .faa genome files. 
The results are summarized in the table below:


| Number of nodes | Parallelizations within a node | Time to finish 50 genomes |
|-----------------|-------------------------------|---------------------------|
| 1               | 1                             | N.A. (20 finished)        |
| 1               | 2                             | N.A. (16 finished)        |
| 1               | 5                             | N.A. (20 finished)        |
| 1               | 10                            | N.A. (17 finished)        |
| 2               | 1                             | N.A. (23 finished)        |
| 5               | 1                             | N.A. (43 finished)        |
| 10              | 1                             | N.A. (39 finished)        |

Each node has 3 hrs of wall time.