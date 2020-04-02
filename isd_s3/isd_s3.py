#!/usr/bin/env python3
"""Interacts with s3 api.

Usage:
```
>>> isd_s3.py -h
usage: isd_s3 [-h] [--noprint] [--prettyprint] [--use_local_config]
              [--s3_url <url>]
              {list_buckets,lb,delete,dl,get_object,go,upload_mult,um,upload,ul,disk_usage,du,list_objects,lo,get_metadata,gm}
              ...

CLI to interact with s3.
Note: To use optional arguments, put them before sub-command.

optional arguments:
  -h, --help            show this help message and exit
  --noprint, -np        Do not print result of actions.
  --prettyprint, -pp    Pretty print result
  --use_local_config, -ul
                        Use your local credentials. (~/.aws/credentials)
  --s3_url <url>        S3 url. Default: 'https://stratus.ucar.edu'

Actions:
  {list_buckets,lb,delete,dl,get_object,go,upload_mult,um,upload,ul,disk_usage,du,list_objects,lo,get_metadata,gm}
                        Use `tool [command] -h` for more info on command
    list_buckets (lb)   lists Buckets
    delete (dl)         Delete objects
    get_object (go)     Pull object from store
    upload_mult (um)    Upload multiple objects.
    upload (ul)         Upload objects
    disk_usage (du)     Reports disc usage from objects
    list_objects (lo)   List objects
    get_metadata (gm)   Get Metadata of object
```
"""
import pdb
import sys
import os
import argparse
import json
import re
import boto3
import logging
import multiprocessing

logging.getLogger("isd_s3")
_is_imported = False
S3_url_env = 'S3_URL'
credentials_file_env = 'AWS_SHARED_CREDENTIALS_FILE'
# to use different object store, change S3_URL environment variable
S3_URL = 'https://stratus.ucar.edu'
if S3_url_env in os.environ:
    S3_URL = os.environ[S3_url_env]

client = None

DEFAULT_BUCKET='rda-data'

# To use different profile, change AWS_PROFILE environment variable
if credentials_file_env not in os.environ:
    os.environ[credentials_file_env] = '/glade/u/home/rdadata/.aws/credentials'


def _get_session(use_local_cred=False, _endpoint_url=S3_URL):
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
    description = "CLI to interact with s3.\nNote: To use optional arguments, place argument them before sub-command."
    parser = argparse.ArgumentParser(
            prog='isd_s3',
            description=description,
            formatter_class=argparse.RawDescriptionHelpFormatter)

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
            action='store_true',
            help="Use your local credentials. (~/.aws/credentials)")
    parser.add_argument('--s3_url',
            type=str,
            required=False,
            metavar='<url>',
            help="S3 url. Default: 'https://stratus.ucar.edu'")

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

    get_parser = actions_parser.add_parser("get_object",
            aliases=['go'],
            help='Pull object from store',
            description='Pull object from store')
    get_parser.add_argument('--key', '-k',
            type=str,
            metavar='<key>',
            required=True,
            help="Object key to pull")
    get_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Bucket from which to pull object")

    upload_mult_parser = actions_parser.add_parser("upload_mult",
            aliases=['um'],
            help='Upload multiple objects.',
            description='Upload multiple objects.')
    upload_mult_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Destination bucket.")
    upload_mult_parser.add_argument('--local_dir', '-ld',
            type=str,
            metavar='<directory>',
            required=True,
            help="Directory to search for files.")
    upload_mult_parser.add_argument('--key_prefix', '-kp',
            type=str,
            metavar='<prefix>',
            required=False,
            default="",
            help="Prepend this string to key")
    upload_mult_parser.add_argument('--recursive', '-r',
            action='store_true',
            required=False,
            help="recursively search directory")
    upload_mult_parser.add_argument('--dry_run', '-dr',
            action='store_true',
            required=False,
            help="Does not upload files.")
    upload_mult_parser.add_argument('--ignore', '-i',
            type=str,
            metavar='<ignore str>',
            nargs='*',
            default=[],
            required=False,
            help="directory to search for files")
    upload_mult_parser.add_argument('--metadata', '-md',
            type=str,
            metavar='<dict str, or path to script>',
            required=False,
            help="Optionally provide metadata for an object. \
                    This can be a function where file is passed.")

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

    du_parser = actions_parser.add_parser("disk_usage",
            aliases=['du'],
            help='Reports disc usage from objects',
            description='List objects')
    du_parser.add_argument('--regex', '-re',
            type=str,
            metavar='<regex>',
            required=False,
            help="Regular expression to match keys against")
    du_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Bucket to list objects from")
    du_parser.add_argument('prefix',
            type=str,
            nargs='?',
            metavar='<prefix string>',
            default="",
            help="prefix to filter objects. E.g. ds084.1/test")
    du_parser.add_argument('--block_size', '-k',
            type=str,
            metavar='<block size>',
            required=False,
            default="1MB",
            help="Specify block size, e.g. 1KB, 500MB, etc")


    lo_parser = actions_parser.add_parser("list_objects",
            aliases=['lo'],
            help='List objects',
            description='List objects')
    lo_parser.add_argument('--regex', '-re',
            type=str,
            metavar='<regex>',
            required=False,
            help="Regular expression to match keys against")
    lo_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=True,
            help="Bucket to list objects from")
    lo_parser.add_argument('prefix',
            type=str,
            nargs='?',
            metavar='<prefix string>',
            default="",
            help="prefix to filter objects. E.g. ds084.1/test")
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

def directory_list(bucket, prefix="", ls=False, keys_only=False):
    """Lists directories using a prefix, similar to POSIX ls

    Args:
        bucket (str): Name of s3 bucket.
        prefix (str): Prefix from which to filter.
        keys_only (bool): Only return the keys.
    """
    if prefix is None:
        prefix = ""
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    if 'CommonPrefixes' in response:
        return list(map(lambda x: x['Prefix'], response['CommonPrefixes']))
    if 'Contents' in response:
        return list(map(lambda x: x['Key'], response['Contents']))
    return [] # Can't find anything

def parse_block_size(block_size_str):
    """Gets the divisor for number of bytes given string.

    Example:
        '1MB' yields 1000000

    """
    units = {
            'KB' : 1000,
            'MB' : 1000000,
            'GB' : 1000000000,
            'TB' : 1000000000000,
            }
    if len(block_size_str) < 3:
        print('block_size doesn\'t have enough information')
        print('defaulting to 1KB')
        block_size_str = '1KB'
    unit = block_size_str[-2:].upper()
    number = int(block_size_str[:-2])
    if unit not in units:
        print('unrecognized unit.')
        print('defaulting to 1KB')
        unit = 'KB'
        number = 1

    base_divisor = units[unit]
    divisor = base_divisor * number
    return divisor


def disk_usage(bucket, prefix="", regex=None, block_size='1MB'):
    """Returns the disk usage for a set of objects.

    Args:
        bucket (str): Name of s3 bucket.
        prefix (str): Prefix from which to filter.

    Returns (dict): disk usage of objects>

    """
    contents = list_objects(bucket, prefix, regex=regex)
    total = 0
    divisor = parse_block_size(block_size)
    for _object in contents:
        total += _object['Size'] / divisor
    return {'disk_usage':total,'units':block_size}

def list_objects(bucket, prefix="", ls=False, keys_only=False, regex=None):
    """Lists objects from a bucket, optionally matching _prefix.

    prefix should be heavily preferred.

    Args:
        bucket (str): Name of s3 bucket.
        prefix (str): Prefix from which to filter.
        ls (bool): Get 'directories'.
        keys_only (bool): Only return the keys.

    Returns:
        (list) : list of objects in given bucket
    """

    if ls:
        return directory_list(bucket, prefix, keys_only)

    contents = []

    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        return []
    contents.extend(response['Contents'])
    while response['IsTruncated']:
        response = client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=response['NextContinuationToken'])
        contents.extend(response['Contents'])
    if regex is not None:
        contents = regex_filter(contents, regex)
    if keys_only:
        return list(map(lambda x: x['Key'], contents))

    return contents

def regex_filter(contents, regex_str):
    """Filters contents using regular expression.

    Args:
        contents (list): response 'Contents' objects
        regex_str (str): regular expression string

    Returns:
        (list) Contents objects.

    """
    filtered_objects = []
    regex = re.compile(regex_str)
    for _object in contents:
        match = regex.match(_object['Key'])
        if match is not None:
            filtered_objects.append(_object)

    return filtered_objects




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
         meta_dict['Metadata'] = json.loads(metadata)
    elif type(metadata) is dict:
        #TODO assert it's a flat dict
        meta_dict['Metadata'] = metadata

    return client.upload_file(local_file, bucket, key, ExtraArgs=meta_dict)

def _get_filelist(local_dir, recursive=False, ignore=[]):
    """Returns local filelist.

    Args:
        local_dir (str): local directory to scan
        recursive (bool): whether or not to recursively scan directory.
                          Does not follow symlinks.
        ignore (iterable[str]): strings to ignore.
    """
    filelist = []
    for root,_dir,files in os.walk(local_dir, topdown=True):
        for _file in files:
            full_filename = os.path.join(root,_file)

            ignore_cur_file=False
            for ignore_str in ignore:
                if ignore_str in full_filename:
                    ignore_cur_file=True
                    break
            if not ignore_cur_file:
                filelist.append(full_filename)
        if not recursive:
            return filelist
    return filelist

def upload_mult_objects(bucket, local_dir, key_prefix="", recursive=False, ignore=[], metadata=None, dry_run=False):
    """Uploads files within a directory.

    Uses key from local files.

    Args:
        bucket (str): Name of s3 bucket.
        local_dir (str): Name of directory to upload
        key_prefix (str): string to prepend to key.
            example: If file is 'test/file.txt' and prefix is 'mydataset/'
                     then, full key would be 'mydataset/test/file.txt'
        recursive (bool): Recursively search directory,
        ignore (iterable[str]): does not upload if string matches
        metadata (func or str): If func, execute giving filename as argument. Expects
                                metadata return code.
                                If json str, all objects will have this placed in it.
                                If location of script, calls script and captures output as
                                the value of metadata.

    Returns:
        None

    """
    filelist = _get_filelist(local_dir, recursive, ignore)
    if metadata is not None:
        func = _interpret_metadata_str(metadata)
    cpus = multiprocessing.cpu_count()
    for _file in filelist:
        key = key_prefix + _file

        metadata_str = None
        if metadata is not None:
            metadata_str = func(_file)

        if dry_run:
            print('(Dry Run) Uploading :'+_file+" to "+bucket+'/'+key)
        else:
            p = multiprocessing.Process(
                    target=upload_object,
                    args=(bucket,_file,key,metadata_str ))
            p.start()
            p.join()


def _interpret_metadata_str(metadata):
    """Determine what metadata string is,
    is it static json, an external script, or python func."""

    if callable(metadata):
        return metadata

    # If it's not a function, it better be a string
    assert isinstance(metadata, str)

    # Check if json
    try:
        metadata_obj = json.loads(metadata)
        return lambda x: metadata_obj
    # Otherwise, it should be a script
    except ValueError:
        import subprocess
        def metadata_func(filename):
            metadata_str = subprocess.check_output(['./'+metadata,filename])
            return json.loads(metadata_str)
        return metadata_func


def delete(bucket, key):
    """Deletes Key from given bucket.

    Args:
        bucket (str): Name of s3 bucket.
        key (str): Name of s3 object key.

    Returns:
        None
    """
    return client.delete_object(Bucket=bucket, Key=key)

def get_object(bucket, key, write_dir='./'):
    """Get's object from store.

    Writes to local dir

    Args:
        bucket (str): Name of s3 bucket.
        key (str): Name of s3 object key.
        write_dir (str): directory to write file to.

    Returns:
        None
    """
    local_filename = os.path.basename(key)
    client.download_file(bucket, key, local_filename)

def delete_mult(bucket, obj_regex=None, dry_run=False):
    """delete objects where keys match regex.

    Args:
        bucket (str): Name of s3 bucket.
        regex (str): Regular expression to match agains
    """
    all_keys = list_objects(bucket, regex=obj_regex, keys_only=True)
    matching_keys = []
    for key in all_objs:
        if dry_run:
            print('Deleting:' + bucket + '/' + key)
        else:
            delete(bucket, key)

def search_metadata(bucket, obj_regex=None, metadata_key=None):
    """Search metadata. Narrow search using regex for keys.

    Args:
        bucket (str): Name of s3 bucket.
        regex (str): Regular expression to narrow search

    Returns:
        (list): keys that match
    """
    all_keys = list_objects(bucket, regex=obj_regex, keys_only=True)
    matching_keys = []
    for key in all_keys:
        return_dict = get_metadata(bucket, key)
        if metadata_key in return_dict.keys():
            matching_keys.append(key)

    return matching_keys


def _get_action_map():
    """Gets a map between the command line 'commands' and functions.

    TODO: Maybe parse the parser?? parser._actions[-1].choices['upload']._actions

    Returns:
        (dict): dict where keys are command strings and values are functions.
    """
    _map = {
            "get_object" : get_object,
            "go" : get_object,
            "list_buckets" : list_buckets,
            "lb" : list_buckets,
            "list_objects" : list_objects,
            "lo" : list_objects,
            "get_metadata" : get_metadata,
            "gm" : get_metadata,
            "upload" : upload_object,
            "ul" : upload_object,
            "delete" : delete,
            "dl" : delete,
            "disk_usage" : disk_usage,
            "du" : disk_usage,
            "upload_mult" : upload_mult_objects,
            "um" : upload_mult_objects
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
    client = _get_session(args.use_local_config)

    func_map = _get_action_map()
    command = args.command
    prog = func_map[command]

    args_dict = args.__dict__
    _remove_common_args(args_dict)
    return prog(**args_dict)

def _pretty_print(struct, pretty_print=True):
    """pretty print output struct"""
    if struct is None:
        pass
    elif pretty_print:
        print(json.dumps(struct, indent=4, default=lambda x: x.__str__()))
    else:
        print(json.dumps(struct, default=lambda x: x.__str__()))

def _exit(error):
    """Throw error or exit.

    Args:
        error (str): Error message.
    """
    if _is_imported:
        raise ISD_S3_Exception(str(error))
    else:
        sys.stdout.write(str(error))
        exit(1)

class ISD_S3_Exception(Exception):
    pass

def main(*args_list):
    """Use command line-like arguments to execute

    Args:
        args_list (unpacked list): list of args as they would be passed to command line.

    Returns:
        (dict, generally) : result of argument call.
    """
    parser = _get_parser()
    args_list = list(args_list) # args_list is tuple
    if len(args_list) == 0:
        parser.print_help()
        _exit(1)
    args = parser.parse_args(args_list)
    noprint = args.noprint
    pretty_print = args.prettyprint
    if args.use_local_config is True:
        # Default loacation is ~/.aws/credentials
        del os.environ['AWS_SHARED_CREDENTIALS_FILE']
    if args.s3_url is not None:
        S3_URL = args.s3_url


    ret = do_action(args)
    if not noprint:
        if pretty_print:
            _pretty_print(ret)
        else:
            _pretty_print(ret, False)
    return ret

if __name__ == "__main__":
    main(*sys.argv[1:])
else:
    client = _get_session()
    _is_imported = True

