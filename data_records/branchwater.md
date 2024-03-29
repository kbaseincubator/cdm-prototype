Get mastiff:
```
curl -o mastiff -L https://github.com/sourmash-bio/mastiff/releases/latest/download/mastiff-client-x86_64-unknown-linux-musl
```
Version: 0.1.0


Find files:

```
find -L /global/cfs/cdirs/kbase/collections/collectionssource/NONE/CDM/f__Rhodanobacteraceae/ -name '*.fna*' > f__Rhodanobacteraceae_input_files.txt
```

Run mastiff
```
(nersc-python) gaprice@perlmutter:login36:~/mash/branchwater_mastiff/CDM_tests> ipython
Python 3.11.7 | packaged by conda-forge | (main, Dec 23 2023, 14:43:09) [GCC 12.3.0]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.20.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import pathlib

In [2]: import subprocess

In [3]: with open("f__Rhodanobacteraceae_input_files.txt") as f:
   ...:     for l in f:
   ...:         l = l.strip()
   ...:         name = pathlib.Path(l).name
   ...:         print(name)
   ...:         subprocess.run(["../mastiff", "-o", name, l])
```

Fix file names
```
(nersc-python) gaprice@perlmutter:login36:~/mash/branchwater_mastiff/CDM_tests> ipython
Python 3.11.7 | packaged by conda-forge | (main, Dec 23 2023, 14:43:09) [GCC 12.3.0]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.20.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import pathlib

In [2]: files = !ls *.fna.gz

In [3]: len(files)
Out[3]: 129

In [6]: for f in files:
   ...:     pathlib.Path(f).rename(pathlib.Path(f).stem + ".mastiff")
   ...: 
```

Munge IDs
```
(nersc-python) gaprice@perlmutter:login36:~/mash/branchwater_mastiff/CDM_tests> ipython
Python 3.11.7 | packaged by conda-forge | (main, Dec 23 2023, 14:43:09) [GCC 12.3.0]
Type 'copyright', 'credits' or 'license' for more information
IPython 8.20.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: import pathlib

In [2]: files = !ls *.fna.mastiff

In [3]: len(files)
Out[3]: 129

In [7]: for f in files:
   ...:     with open(f) as in_, open(pathlib.Path(f).stem + ".ids.mastiff", 'w') as out:
   ...:         in_.readline()  # skip header
   ...:         for l in in_:
   ...:             parts = l.strip().split(',')
   ...:             id_ = "_".join(parts[2].split('/')[-1].split("_")[:2])
   ...:             out.write(','.join(parts[:2] + [id_]) + '\n')
```

Get minio client:
```
wget https://dl.min.io/client/mc/release/linux-amd64/mc
(nersc-python) gaprice@perlmutter:login36:~> minio/mc --version
mc version RELEASE.2024-03-25T16-41-14Z (commit-id=7bac47fe04a4a26faa0e8515036f7ff2dfc48c75)
Runtime: go1.21.8 linux/amd64
Copyright (c) 2015-2024 MinIO, Inc.
License GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>
```

Set up connection as per https://kbase-jira.atlassian.net/browse/DEVOPS-1779

Load to Minio:
```
gaprice@perlmutter:login27:~/mash/branchwater_mastiff/CDM_tests> ~/minio/mc cp ./*.ids.mastiff cdm-minio/cdm/branchwater/Rhodanobacteraceae/
...enomic.fna.ids.mastiff: 491.37 MiB / 491.37 MiB ┃▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓┃ 56.44 MiB/s 8s
```
