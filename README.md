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
