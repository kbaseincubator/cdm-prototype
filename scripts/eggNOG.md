# eggNOG tool

## Execution

- Execute the eggNOG tool leveraging the framework developed for the collections project.

    ```bash
    tool=eggnog
    env=NONE
    kbase_collection=CDM
    source_ver=f__Rhodanobacteraceae
    load_ver=f__Rhodanobacteraceae
    source_file_ext=protein.faa.gz
    
    cd /global/cfs/cdirs/kbase/collections/collections
    PYTHONPATH=. python src/loaders/jobs/taskfarmer/task_generator.py \
        --tool $tool \
        --env $env \
        --kbase_collection $kbase_collection \
        --source_ver $source_ver \
        --load_ver $load_ver \
        --source_file_ext $source_file_ext \
        --submit_job
    ```

- We did some testing to determine the optimal parallelization and threads per instance for the eggNOG tool. The following are the results of the testing.
    ```text
    64 parallelization 4 threads per instance
    Node becomes unresponsive
    
    32 parallelization 4 threads per instance
    Program intermittently disrupted
    
    32 parallelization 16 threads per instance 
    Used 87.93 minutes to execute eggnog for 147 data units
    
    16 parallelization 16 threads per instance
    Used 16.29 minutes to execute eggnog for 147 data units
    
    16 parallelization 8 threads per instance
    Used 13.88 minutes to execute eggnog for 147 data units
    
    16 parallelization 4 threads per instance
    Used 16.2 minutes to execute eggnog for 147 data units
    
    4 parallelization 16 threads per instance
    Used 24.86 minutes to execute eggnog for 147 data units
    ```

- Please note:

    To enhance execution efficiency, we opted for utilizing 16 parallel processes.
    We also manually adjust the 'threads' variable in task_generator.py, as the default is 32. You can find the relevant code [here](https://github.com/kbase/collections/blob/develop/src/loaders/jobs/taskfarmer/task_generator.py)

## Results
Copy the result files to minIO
```bash
cd /global/cfs/cdirs/kbase/collections/collectionsdata/NONE/CDM/f__Rhodanobacteraceae/eggnog/batch_no_batch_size_129_node_job_0
mc cp -r ./ cdm-minio/cdm/eggnog/Rhodanobacteraceae
```