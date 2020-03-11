# rda-s3
NCAR RDA S3 Object Storage utility

A command line tool to manage data access to the NCAR RDA S3 Object Storage
system.

Source Code: [https://github.com/NCAR/rda-s3](https://github.com/NCAR/rda-s3)


- [Usage](#Usage)
  + [Default help](#default-help)
  + [Command Help](#command-specific-usage)
    - [List Buckets](#list-buckets)
    - [Delete](#delete)
    - [Upload](#upload)
    - [List Objects](#list-objects)
    - [Get Metadata](#get-metadata)
- [Examples](#Examples)
  + [List Buckets](#list-buckets)
  

## Usage

#### Default help

```
./rda_s3.py
CLI to interact with s3.

optional arguments:
  -h, --help            show this help message and exit
  --noprint, -np        Do not print result of actions.
  --prettyprint, -pp    Pretty print result
  --use_local_config USE_LOCAL_CONFIG, -ul USE_LOCAL_CONFIG
                        Use your local credentials. (~/.aws/credentials)

Actions:
  {list_buckets,lb,delete,dl,upload,ul,list_objects,lo,get_metadata,gm}
                        Use `tool [command] -h` for more info on command
    list_buckets (lb)   lists Buckets
    delete (dl)         Delete objects
    upload (ul)         Upload objects
    list_objects (lo)   List objects
    get_metadata (gm)   Get Metadata of object
```

#### Command Specific Usage

##### List buckets

```
./rda_s3.py list_buckets -h 
usage: rda_s3 list_buckets [-h] [--buckets_only]

Lists buckets

optional arguments:
  -h, --help           show this help message and exit
  --buckets_only, -bo  Only return the bucket names
```

##### Delete

```
./rda_s3.py delete -h
usage: rda_s3 delete [-h] --key <key> --bucket <bucket>

Delete objects

optional arguments:
  -h, --help            show this help message and exit
  --key <key>, -k <key>
                        Object key to delete
  --bucket <bucket>, -b <bucket>
                        Bucket from which to delete
rda-s3/rda_s3> 

```

##### Upload

```
./rda_s3.py upload -h
usage: rda_s3 upload [-h] --local_file <filename> --bucket <bucket> --key
                     <key> [--metadata <dict str>]

Upload objects

optional arguments:
  -h, --help            show this help message and exit
  --local_file <filename>, -lf <filename>
                        local file to upload
  --bucket <bucket>, -b <bucket>
                        Destination bucket
  --key <key>, -k <key>
                        key given to object
  --metadata <dict str>, -md <dict str>
                        Optionally provide metadata for an object
```

##### List objects

```
./rda_s3.py list_objects -h
usage: rda_s3 list_objects [-h] --bucket <bucket> [-ls] [--keys_only]
                           [<glob string>]

List objects

positional arguments:
  <glob string>         String to glob from. E.g. ds084.1/*

optional arguments:
  -h, --help            show this help message and exit
  --bucket <bucket>, -b <bucket>
                        Bucket to list objects from
  -ls                   List just the directory level
  --keys_only, -ko      Only return the object keys
```

##### Get metadata

```
./rda_s3.py get_metadata -h

Get Metadata of an object

optional arguments:
  -h, --help            show this help message and exit
  --bucket <bucket>, -b <bucket>
                        Bucket from which to retrieve metadata
  --key <key>, -k <key>
                        Key from which to retrieve metadata.
```


## Examples

##### List Buckets






