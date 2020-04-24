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
import sys
import argparse
import logging

import isd_s3
from isd_s3.isd_s3 import *

logger = logging.getLogger(__name__)

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
            help="S3 url. Default: https://s3.amazonaws.com/")

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
    del _dict['s3_url']
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
    client = get_session(endpoint_url=args.s3_url, use_local_cred=args.use_local_config)

    func_map = _get_action_map()
    command = args.command
    prog = func_map[command]

    args_dict = args.__dict__
    _remove_common_args(args_dict)
    
    # add client to keyword args since it's needed by functions in isd_s3.py
    args_dict.update({'client': client})
    
    return prog(**args_dict)

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
        sys.exit(1)
    args = parser.parse_args(args_list)

    noprint = args.noprint
    pp = args.prettyprint

    if args.use_local_config is True:
        # Default loacation is ~/.aws/credentials
        del os.environ['AWS_SHARED_CREDENTIALS_FILE']
    if args.s3_url is None:
        try:
            args.s3_url = os.environ['S3_URL']
        except KeyError:
            logger.warning("S3 endpoint URL is not defined.  This may be passed via the \
                            argument --s3_url or assigned to the environment variable 'S3_URL'. \
                            Default URL is https://s3.amazonaws.com/.")        

    logger.info('s3_url arg: {}'.format(args.s3_url))

    ret = do_action(args)
    if not noprint:
        if pp:
            pretty_print(ret)
        else:
            pretty_print(ret, False)
    return ret

if __name__ == "__main__":
    main(*sys.argv[1:])
    