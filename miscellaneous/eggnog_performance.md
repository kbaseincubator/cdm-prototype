
## Performance of EggNOG-mapper

Conducted performance tests at NERSC utilizing the Taskfarmer scheduler to process 50 IMG .faa genome files. 
The results are summarized in the table below:


| Number of nodes | Parallelizations within a node | Time to finish 50 genomes | Everage node time per genome |
|-----------------|-------------------------------|---------------------------|------------------------------|
| 1               | 1                             | N.A. (20 finished)        | 9 min                        |
| 1               | 2                             | N.A. (16 finished)        | 11.25 min                    |
| 1               | 5                             | N.A. (20 finished)        | 9 min                        |
| 1               | 10                            | N.A. (17 finished)        | 10.5 min                     |
| 2               | 1                             | N.A. (23 finished)        | 15.5 min                     |
| 5               | 1                             | N.A. (43 finished)        | 21 min                       |
| 10              | 1                             | N.A. (39 finished)        | 42 min                       |

Each node has 3 hrs of wall time.