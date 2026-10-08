# isd-s3
NCAR ISD S3 Object Storage utility.

A command line tool to manage data access to the NCAR S3 Object Storage
system.  Developed by the Information Systems Division (ISD), a division 
within the Computational and Information Systems Laboratory (CISL) at the
National Center for Atmospheric Research (NCAR).

Source Code: [https://github.com/NCAR/isd-s3](https://github.com/NCAR/isd-s3)

### Installation
Install with `pip install ncar-isd-s3`

You can then import the module `isd_s3`.  For example:
```
from isd_s3 import isd_s3
session = isd_s3.Session()
session.list_buckets()
```

### Downloading objects
`isd_s3 go --key <key>` downloads a single object into the current directory
(use `--local_dir`/`-ld` to choose another directory).

With `--recursive`/`-r`, `--key` is treated as a prefix and every object under
it is downloaded. The prefix itself is **not** recreated locally; the structure
below it is preserved and written directly into `--local_dir` (default: the
current directory). For example, with objects `data/2024/a.txt` and
`data/2024/sub/b.txt`:

```
isd_s3 go -r -k data/2024/             # writes ./a.txt and ./sub/b.txt
isd_s3 go -r -k data/2024/ -ld ./2024  # writes ./2024/a.txt and ./2024/sub/b.txt
```

Pass `-ld` to keep the downloaded files in their own directory.
