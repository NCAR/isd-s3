#!/usr/bin/env python
"""Interacts with s3 api

Usage:
```
>>> rda_s3.py -h
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
"""

import pdb
import sys
import os
import argparse
import json
import boto3
import logging

S3_URL = 'https://stratus.ucar.edu'
client = None
logging.getLogger("rda_s3")

credentials_file_env = 'AWS_SHARED_CREDENTIALS_FILE'
if credentials_file_env not in os.environ:
    os.environ[credentials_file_env] = '/glade/u/home/rdadata/.aws/credentials'


def get_session(use_local_cred=False, _endpoint_url=S3_URL):
    """Gets a boto3 session client.
    This should generally be executed after module load.

    Args:
        use_local_cred (bool): Use personal credentials for session. Default False.
        _endpoint_url: url to s3

    Returns:
        (botocore.client.S3): botocore client object
    """
    session = boto3.session.Session()
    return session.client(
            service_name='s3',
            endpoint_url=_endpoint_url
            )

def _get_parser():
    """Creates and returns parser object.

    Returns:
        (argparse.ArgumentParser): Parser object from which to parse arguments.
    """
    description = "CLI to interact with s3."
    parser = argparse.ArgumentParser(prog='rda_s3', description=description)

    # Arguments that are always allowed
    parser.add_argument('--noprint', '-np',
            action='store_true',
            required=False,
            help="Do not print result of actions.")
    parser.add_argument('--prettyprint', '-pp',
            action='store_true',
            required=False,
            help="Pretty print result")
    parser.add_argument('--use_local_config', '-ul',
            required=False,
            help="Use your local credentials. (~/.aws/credentials)")

    # Mutually exclusive commands
    actions_parser = parser.add_subparsers(title='Actions',
            help='Use `tool [command] -h` for more info on command')
    actions_parser.required = True
    actions_parser.dest = 'command'

    # Commands
    lb_parser = actions_parser.add_parser(
            "list_buckets",
            aliases=['lb'],
            help='lists Buckets',
            description='Lists buckets')
    lb_parser.add_argument('--buckets_only', '-bo',
            action='store_true',
            required=False,
            help="Only return the bucket names")

    del_parser = actions_parser.add_parser("delete",
            aliases=['dl'],
            help='Delete objects',
            description='Delete objects')
    del_parser.add_argument('--key', '-k',
            type=str,
            metavar='<key>',
            required=True,
            help="Object key to delete")
    del_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Bucket from which to delete")

    upload_parser = actions_parser.add_parser("upload",
            aliases=['ul'],
            help='Upload objects',
            description='Upload objects')
    upload_parser.add_argument('--local_file', '-lf',
            type=str,
            metavar='<filename>',
            required=True,
            help="local file to upload")
    upload_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Destination bucket")
    upload_parser.add_argument('--key', '-k',
            type=str,
            metavar='<key>',
            required=True,
            help="key given to object ")
    upload_parser.add_argument('--metadata', '-md',
            type=str,
            metavar='<dict str>',
            required=False,
            help="Optionally provide metadata for an object")

    lo_parser = actions_parser.add_parser("list_objects",
            aliases=['lo'],
            help='List objects',
            description='List objects')
    lo_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Bucket to list objects from")
    lo_parser.add_argument('glob',
            type=str,
            nargs='?',
            metavar='<glob string>',
            help="String to glob from. E.g. ds084.1/*")
    lo_parser.add_argument('-ls',
            action='store_true',
            required=False,
            help="List just the directory level")

    lo_parser.add_argument('--keys_only', '-ko',
            action='store_true',
            required=False,
            help="Only return the object keys")

    meta_parser = actions_parser.add_parser("get_metadata",
            aliases=['gm'],
            help='Get Metadata of object',
            description='Get Metadata of an object')
    meta_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Bucket from which to retrieve metadata")
    meta_parser.add_argument('--key', '-k',
            type=str,
            metavar='<key>',
            required=True,
            help="Key from which to retrieve metadata.")

    return parser

def list_buckets(buckets_only=False):
    """Lists all buckets.

    Args:
        buckets_only (bool): Only return bucket names

    Returns:
        (list) : list of buckets.
    """
    response = client.list_buckets()['Buckets']
    if buckets_only:
        return list(map(lambda x: x['Name'], response))
    return response

def list_objects(bucket, glob=None, ls=False, keys_only=False):
    """Lists objects from a bucket, optionally matching _glob.

    _glob should be heavily preferred.

    Args:
        bucket (str): Name of s3 bucket.
        glob (str): Prefix from which to filter.
        ls (bool): Get 'directories'.
        keys_only (bool): Only return the keys.

    Returns:
        (list) : list of objects in given bucket
    """

    if ls:
        # Need a Prefix if using -ls
        if glob is None:
            glob = ""
        response = client.list_objects_v2(Bucket=bucket, Prefix=glob, Delimiter='/')
        if 'CommonPrefixes' in response:
            return list(map(lambda x: x['Prefix'], response['CommonPrefixes']))
        if 'Contents' in response:
            return list(map(lambda x: x['Key'], response['Contents']))
        return [] # Can't find anything

    if glob is None:
        response = client.list_objects_v2(Bucket=bucket)
    else:
        response = client.list_objects_v2(Bucket=bucket, Prefix=glob)

    if keys_only:
        return list(map(lambda x: x['Key'], response['Contents']))
    return response['Contents']

def get_metadata(bucket, key):
    """Gets metadata of a given object key.

    Args:
        bucket (str): Name of s3 bucket.
        key (str): Name of s3 object key.

    Returns:
        (dict) metadata of given object
    """
    return client.head_object(Bucket=bucket, Key=key)['Metadata']

def upload_object(bucket, local_file, key, metadata=None):
    """Uploads files to object store.

    Args:
        bucket (str): Name of s3 bucket.
        local_file (str): Filename of local file.
        key (str): Name of s3 object key.
        metadata (dict, str): dict or string representing key/value pairs.

    Returns:
        None
    """
    if metadata is None:
        return client.upload_file(local_file, bucket, key)

    meta_dict = {'Metadata' : None}
    if type(metadata) is str:
        # Parse string or check if file exists
        pass
    elif type(metadata) is dict:
        #TODO assert it's a flat dict
        meta_dict['Metadata'] = metadata

    return client.upload_file(local_file, bucket, key, ExtraArgs=meta_dict)


def delete(bucket, key):
    """Deletes Key from given bucket.

    Args:
        bucket (str): Name of s3 bucket.
        key (str): Name of s3 object key.

    Returns:
        None
    """
    return client.delete_object(Bucket=bucket, Key=key)

def _get_action_map():
    """Gets a map between the command line 'commands' and functions.

    TODO: Maybe parse the parser?? parser._actions[-1].choices['upload']._actions

    Returns:
        (dict): dict where keys are command strings and values are functions.
    """
    _map = {
            "list_buckets" : list_buckets,
            "lb" : list_buckets,
            "list_objects" : list_objects,
            "lo" : list_objects,
            "get_metadata" : get_metadata,
            "gm" : get_metadata,
            "upload" : upload_object,
            "ul" : upload_object,
            "delete" : delete,
            "d" : delete
            }
    return _map

def _remove_common_args(_dict):
    """Removes global arguments from given dict.

    Args:
        _dict (dict) : dict where common args removed.
        Note that _dict is not copied. Typically from argparse namespace.

    Returns:
        None
    """
    del _dict['noprint']
    del _dict['prettyprint']
    del _dict['use_local_config']
    del _dict['command']

def do_action(args):
    """Interprets the parser and kicks processes command

    Args:
        args (Namespace): Argument parser to find commands and sub-commands.

    Returns:
        None ## Maybe returns (str) or (dict)?
    """
    # Init Session
    global client
    client = get_session(args.use_local_config)

    func_map = _get_action_map()
    command = args.command
    prog = func_map[command]

    args_dict = args.__dict__
    _remove_common_args(args_dict)
    return prog(**args_dict)

def main(args)
    parser = _get_parser()
    if len(sys.argv) == 0:
        parser.print_help()
        exit(1)
    args = parser.parse_args(args)
    noprint = args.noprint
    pretty_print = args.prettyprint
    if args.use_local_config is True:
        del os.environ['AWS_SHARED_CREDENTIALS_FILE']

    ret = do_action(args)
    if not noprint:
        if pretty_print:
            try:
                print(json.dumps(ret, indent=4))
            except:
                print(ret)
        else:
            print(ret)

if __name__ == "__main__":
    main(sys.argv[1:])
else:
    client = get_session()

