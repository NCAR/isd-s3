#!/usr/bin/env python
"""Interacts with s3 api

Usage:
```
TODO:
    get using tool -h
```
"""

import pdb
import sys
import os
import argparse
import logging
import json
import boto3

__author__ = "Tom Cram (tcram@ucar.edu), Riley Conroy (rpconroy@ucar.edu)"
__license__ = "GPL"
__version__ = "0.1.0" # Please refer to https://semver.org/ for versioning.


S3_URL = 'https://stratus.ucar.edu'

client = None
os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '/glade/u/home/rdadata/.aws/credentials'

def get_session(use_local_cred=False, _endpoint_url=S3_URL):
    """Gets a boto3 session client.

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

def get_parser():
    """Creates and returns parser object.

    Returns:
        (argparse.ArgumentParser): Parser object from which to parse arguments.
    """
    description = "CLI to interact with s3."
    parser = argparse.ArgumentParser(prog='tool', description=description)

    # Arguments that are always allowed
    parser.add_argument('--noprint', '-np',
            action='store_true',
            required=False,
            help="Do not print result of actions.")
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


    meta_parser = actions_parser.add_parser("get_metadata",
            aliases=['gm'],
            help='Get Metadata of object',
            description='Get Metadata of an object')

    return parser

def list_buckets():
    """Lists all buckets"""
    pass

def list_objects(bucket, _glob=None):
    """Lists objects from a bucket, optionally matching _glob."""
    pass

def get_metadata(bucket, key):
    """Gets metadata of a given object key."""
    print('get_metadata')
    pass

def upload_object(bucket, local_file, key, metadata=None):
    """Uploads files to object store."""
    pass

def delete(bucket, key):
    """Deletes Key from given bucket."""
    print('delete')

def get_action_map():
    """Gets a map between the command line 'commands' and functions.

    TODO: Maybe parse the parser?? parser._actions[-1].choices['upload']._actions

    Returns:
        (dict): dict where keys are command strings and values are functions.
    """
    _map = {
            "list_buckets" : list_buckets,
            "list_objects" : list_objects,
            "get_metadata" : get_metadata,
            "upload" : upload_object,
            "delete" : delete
            }
# Maybe not this

   # _map = {
   #         "list_buckets" : {
   #             'prog' : list_buckets,
   #             'args' : []
   #             },
   #         "list_objects" : {
   #             'prog' : list_objects,
   #             'args' : ['bucket']
   #             },
   #         "get_metadata" : {
   #             'prog' : get_metadata,
   #             'args' : []
   #             },
   #         "upload" : {
   #             'prog' : upload_object,
   #             'args' : []
   #             },
   #         "delete" : {
   #             'prog' : delete,
   #             'args' : []
   #             }
   #         }
    return _map

def remove_common_args(_dict):
    """Removes global arguments from given dict."""
    del _dict['noprint']
    del _dict['use_local_config']
    del _dict['command']

def do_action(args):
    """Interprets the parser and kicks processes command

    Args:
        args (Namespace): Argument parser to find commands and sub-commands.

    Returns:
        None ## Maybe returns (str) or (dict)?
    """
    func_map = get_action_map()
    command = args.command
    prog = func_map[command]
    args_dict = args.__dict__
    remove_common_args(args_dict)
    prog(**args_dict)

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    do_action(args)
else:
    client = get_session()
