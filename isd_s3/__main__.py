#!/usr/bin/env python
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
  --credentials_file, -cf
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
import os
import sys
import argparse
import logging
import pdb
import json
import select

if __package__ is None or __package__ == "":
    import isd_s3
    import config
else:
    from . import isd_s3
    from . import config

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
    parser.add_argument('--loglevel', '-ll',
            type=str,
            required=False,
            help="Set logging level. DEBUG, INFO, WARNING, ERROR, CRITICAL")
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
    parser.add_argument('--default_bucket', '-db',
            type=str,
            required=False,
            metavar='<bucket name>',
            help="Default bucket")
    parser.add_argument('--credentials_file', '-cf',
            type=str,
            required=False,
            metavar='<credentials file>',
            help="Location of s3 credentials. See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html")

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
    del_parser.add_argument('keys',
            type=str,
            nargs='+',
            metavar='<keys to delete>',
            default=[],
            help="keys to delete")
    del_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=False,
            help="Bucket from which to delete")

    del_mult_parser = actions_parser.add_parser("delete_mult",
            aliases=['dm'],
            help='Delete multiple objects',
            description='Delete multiple objects')
    del_mult_parser.add_argument('--prefix',
            type=str,
            metavar='<prefix to delete>',
            required=True,
            help="""Prefix to delete. For example:
            --prefix '/path/to/dir/'
            would delete all keys that start with that prefix.""")
    del_mult_parser.add_argument('--recursive',
            action='store_true',
            required=False,
            help="Recursively delete from prefix")
    del_mult_parser.add_argument('--dry_run', '-dr',
            action='store_true',
            required=False,
            help="Does not delete files. This is used to test whether the correct files are being selected for deletion.")
    del_mult_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=False,
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
    get_parser.add_argument('--local_filename', '-lf',
            type=str,
            metavar='<local filename>',
            required=False,
            help="Save to another name than the key")
    get_parser.add_argument('--local_dir', '-ld',
            type=str,
            metavar='<local directory>',
            default='./',
            required=False,
            help="Save to another specified directory, rather than current working directory.")
    get_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=False,
            help="Bucket from which to pull object.")

    upload_mult_parser = actions_parser.add_parser("upload_mult",
            aliases=['um'],
            help='Upload multiple objects.',
            description='Upload multiple objects.')
    upload_mult_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=False,
            help="Destination bucket.")
    upload_mult_parser.add_argument('--local_dir', '-ld',
            type=str,
            metavar='<directory>',
            required=True,
            help="Directory to search for files which will be uploaded.")
    upload_mult_parser.add_argument('--key_prefix', '-kp',
            type=str,
            metavar='<prefix>',
            required=False,
            default="",
            help="Prepend this string to key")
    upload_mult_parser.add_argument('--recursive', '-r',
            action='store_true',
            required=False,
            help="Recursively search directory and uploads all found files. Preserves directory structure.")
    upload_mult_parser.add_argument('--dry_run', '-dr',
            action='store_true',
            required=False,
            help="Does not upload files. This is used to test whether the correct files are being selected for upload.")
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

    replace_parser = actions_parser.add_parser("replace_metadata",
            help='replace object metadata',
            description='replace objects metadata')
    replace_parser.add_argument('--bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=False,
            help="Name of bucket")
    replace_parser.add_argument('--key', '-k',
            type=str,
            metavar='<key>',
            required=True,
            help="key given to object ")
    replace_parser.add_argument('--metadata', '-md',
            type=str,
            metavar='<dict str>',
            required=False,
            help="Provide metadata for an object. Otherwise deletes metadata")

    move_parser = actions_parser.add_parser("move_object",
            aliases=['mv'],
            help='move object to new key',
            description='move object')
    move_parser.add_argument('--source_key', '-k',
            type=str,
            metavar='<source key>',
            required=True,
            help="Object key to move")
    move_parser.add_argument('--dest_key', '-dk',
            type=str,
            metavar='<new key>',
            required=True,
            help="Object key to move")
    move_parser.add_argument('--source_bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=False,
            help="source bucket")
    move_parser.add_argument('--dest_bucket', '-db',
            type=str,
            metavar='<destination bucket>',
            required=False,
            help="destination bucket")
    move_parser.add_argument('--metadata', '-md',
            type=str,
            metavar='<dict str>',
            required=False,
            help="Provide metadata for an object. Otherwise deletes metadata")

    copy_parser = actions_parser.add_parser("copy_object",
            aliases=['cp'],
            help='copy object to new key',
            description='copy object')
    copy_parser.add_argument('--source_key', '-k',
            type=str,
            metavar='<source key>',
            required=True,
            help="Object key to copy")
    copy_parser.add_argument('--dest_key', '-dk',
            type=str,
            metavar='<new key>',
            required=True,
            help="Object key to copy")
    copy_parser.add_argument('--source_bucket', '-b',
            type=str,
            metavar='<bucket>',
            required=False,
            help="source bucket")
    copy_parser.add_argument('--dest_bucket', '-db',
            type=str,
            metavar='<destination bucket>',
            required=False,
            help="destination bucket")
    copy_parser.add_argument('--metadata', '-md',
            type=str,
            metavar='<dict str>',
            required=False,
            help="Provide metadata for an object. Otherwise deletes metadata")

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
            required=False,
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
    upload_parser.add_argument('--md5',
            action='store_true',
            required=False,
            help="Compute ContentMD5 before uploading")

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
            required=False,
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
            required=False,
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
            required=False,
            help="Bucket from which to retrieve metadata")
    meta_parser.add_argument('--key', '-k',
            type=str,
            metavar='<key>',
            required=True,
            help="Key from which to retrieve metadata.")

    return parser

def _get_action(obj, command):
    """Gets a map between the command line 'commands' and functions.

    TODO: Maybe parse the parser?? parser._actions[-1].choices['upload']._actions

    Returns:
        (dict): dict where keys are command strings and values are functions.
    """
    # If command isn't the same as the method use map
    command_map = {
            "go" : 'get_object',
            "lb" : 'list_buckets',
            "lo" : 'list_objects',
            "gm" : 'get_metadata',
            "upload" : 'upload_object',
            "ul" : 'upload_object',
            "cp" : 'copy_object',
            "mv" : 'move_object',
            "dl" : 'delete',
            "dm" : 'delete_mult',
            "du" : 'disk_usage',
            "upload_mult" : 'upload_mult_objects',
            "um" : 'upload_mult_objects'
            }
    if command in command_map:
        command = command_map[command]

    func = getattr(obj, command)
    return func

def get_global_args():
    global_args = [
            'noprint',
            'prettyprint',
            's3_url',
            'use_local_config',
            'command',
            'default_bucket',
            'loglevel',
            'credentials_file']
    return global_args

def _remove_common_args(_dict):
    """Removes global arguments from given dict.

    Args:
        _dict (dict) : dict where common args removed.
        Note that _dict is not copied. Typically from argparse namespace.

    Returns:
        None
    """
    args = get_global_args()
    for arg in args:
        _dict.pop(arg, None)

def do_action(args):
    """Interprets the parser and kicks processes command

    Args:
        args (Namespace): Argument parser to find commands and sub-commands.

    Returns:
        function
    """
    # Init Session
    session = isd_s3.Session(endpoint_url=args.s3_url, credentials_loc=args.credentials_file)

    # Get function corresponding with command
    function = _get_action(session, args.command)

    # Remove global arguments
    args_dict = args.__dict__
    _remove_common_args(args_dict)


    return function(**args_dict)

def _pretty_print(struct, pretty_print=True):
    """pretty print output struct"""
    if struct is not None:
        if pretty_print:
            print(json.dumps(struct, indent=4, default=lambda x: x.__str__()))
        else:
            print(json.dumps(struct, default=lambda x: x.__str__()))

def flatten_dict(_dict):
    assert 'command' in _dict
    args_list = [_dict['command']]
    positional_arguments = None
    del _dict['command']
    global_args = get_global_args()
    for k, v in _dict.items():
        if k == 'positional_arguments':
            positional_arguments = v
            continue
        if k[0] != '-':
            k = '-'+k
        insert_pos = len(args_list) + 100
        if k[2:] in global_args:
            insert_pos=0
            args_list.insert(insert_pos, k)
            insert_pos += 1
        else:
            args_list.insert(insert_pos, k)

        if isinstance(v, str):
            args_list.insert(insert_pos, v)
        elif isinstance(v, list):
            for item in v:
                args_list.insert(insert_pos, item)
                insert_pos += 1
        elif isinstance(v, dict):
            args_list.append(insert_pos, json.dumps(v))
        elif isinstance(v, bool):
            pass
        else:
            raise Exception("Unrecognized value")
    if positional_arguments is not None:
        args_list.extend(positional_arguments)
    return args_list

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
    # Need to have a default s3_url
    if args.s3_url is None and config.get_s3_url() is None:
        args.s3_url = config.get_default_environment()['s3_url']
    if args.loglevel is not None:
        level = getattr(logging, args.loglevel.upper())
        logger.setLevel(level)


    config.configure_environment(args.s3_url, args.credentials_file, args.default_bucket)

    result_json = do_action(args)
    print_output(result_json, pp, noprint)
    return result_json

def print_output(output, pretty_print=True, noprint=False):
    if not noprint:
        if pretty_print:
            _pretty_print(output)
        else:
            _pretty_print(output, False)

def read_json_from_stdin():
    """Read arguments from stdin"""
    in_json=""
    for line in sys.stdin.readlines():
        in_json += line
    json_dict = json.loads(in_json)
    return json_dict

def call_action_from_dict(args_dict):
    """calls action using dict instead of command line arguments."""
    assert 'command' in args_dict

   # if args.use_local_config is True:
   #     # Default loacation is ~/.aws/credentials
   #     del os.environ['AWS_SHARED_CREDENTIALS_FILE']
   # # Need to have a default s3_url
   # if args.s3_url is None and config.get_s3_url() is None:
   #     args.s3_url = config.get_default_environment()['s3_url']

    if 's3_url' not in args_dict:
        args_dict['s3_url'] = None
    if 'credentials_file' not in args_dict:
        args_dict['credentials_file'] = None
    session = isd_s3.Session(endpoint_url=args_dict['s3_url'], credentials_loc=args_dict['credentials_file'])

    # Get function corresponding with command
    function = _get_action(session, args_dict['command'])

    # Remove global arguments
    _remove_common_args(args_dict)
    result = function(**args_dict)
    print_output(result)

    return result

if __name__ == "__main__":
    #from_pipe = not os.isatty(sys.stdin.fileno())
    from_pipe = select.select([sys.stdin,],[],[],0.0)[0]
    if len(sys.argv) > 1:
        main(*sys.argv[1:])
    elif from_pipe:
        json_input = read_json_from_stdin()
        if isinstance(json_input, list):
            for command_json in json_input:
                main(*flatten_dict(command_json))
        else:
            main(*flatten_dict(json_input))
       # call_action_from_dict(json_input)
    else:
        main(*sys.argv[1:])


