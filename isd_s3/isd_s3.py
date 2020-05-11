#!/usr/bin/env python3
"""Interacts with s3 api.

Example usage:
```
>>> from isd_s3 import isd_s3
>>> client = isd_s3.get_session()
>>> isd_s3.list_buckets(client)
```
"""

import pdb
import sys
import os
import json
import re
import boto3
import logging
import multiprocessing

logger = logging.getLogger(__name__)

class Session(object):

    def __init__(self, credentials_loc=None, endpoint_url=None, default_bucket=None, config_file=None):


        self.endpoint_url = self.get_endpoint_url(endpoint_url)
        self.client = self.get_session(endpoint_url)

    def get_endpoint_url(self, endpoint_url=None):
        pass

    def get_session(self, endpoint_url ):
        """Gets a boto3 session client.
        This should generally be executed after module load.

        Args:
            use_local_cred (bool): Use personal credentials for session. Default False.
            endpoint_url: url to s3. Default https://s3.amazonaws.com/

        Returns:
            (botocore.client.S3): botocore client object

        See boto3 session and client reference at
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
        """
        try:
            endpoint_url = kwargs['endpoint_url']
        except KeyError:
            endpoint_url = None

        session = boto3.session.Session()
        return session.client(
                service_name='s3',
                endpoint_url=endpoint_url
                )

    def list_buckets(**kwargs):
        """Lists all buckets.

        Args:
            client (botocore.client.S3) [REQUIRED]: boto3 client created by get_session()
            buckets_only (bool): Only return bucket names. Default False.

        Returns:
            (list) : list of buckets.
        """
        if 'client' not in kwargs:
        	raise KeyError("{}.list_buckets() requires keyword argument 'client'".format(__name__))
        else:
            client = kwargs['client']
        try:
            buckets_only = kwargs['buckets_only']
        except KeyError:
            buckets_only = False

        response = client.list_buckets()['Buckets']
        if buckets_only:
            return list(map(lambda x: x['Name'], response))
        return response

    def directory_list(bucket=None, client=None, prefix="", ls=False, keys_only=False):
        """Lists directories using a prefix, similar to POSIX ls

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            client (botocore.client.S3) [REQUIRED]: boto3 client created by get_session()
            prefix (str): Prefix from which to filter.
            ls (bool): Defaut False
            keys_only (bool): Only return the keys.  Default False.
        """
        if bucket is None:
            raise TypeError("{}.directory_list() requires keyword argument 'bucket'".format(__name__))
        if client is None:
            raise TypeError("{}.directory_list() requires keyword argument 'client'".format(__name__))

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


    def disk_usage(bucket=None, client=None, prefix="", block_size='1MB'):
        """Returns the disk usage for a set of objects.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            client (botocore.client.S3) [REQUIRED]: boto3 session client created by get_session()
            prefix (str): Prefix from which to filter.
            regex (str): regex string.  Default None
            block_size (str): block size

        Returns (dict): disk usage of objects>

        """
        if 'bucket' is None:
        	raise TypeError("{}.disk_usage() requires keyword argument 'bucket'".format(__name__))
        if 'client' is None:
        	raise TypeError("{}.disk_usage() requires keyword argument 'client'".format(__name__))

        contents = list_objects(bucket, prefix, client=client, regex=regex)
        total = 0
        divisor = parse_block_size(block_size)
        for _object in contents:
            total += _object['Size'] / divisor
        return {'disk_usage':total,'units':block_size}

    def list_objects(bucket=None, client=None, prefix="", ls=False, keys_only=False):
        """Lists objects from a bucket, optionally matching _prefix.

        prefix should be heavily preferred.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            client (botocore.client.S3) [REQUIRED]: boto3 session client created by get_session()
            prefix (str): Prefix from which to filter.
            ls (bool): Get 'directories'.
            keys_only (bool): Only return the keys.
            regex (str): regex string

        Returns:
            (list) : list of objects in given bucket
        """
        if 'bucket' is None:
        	raise TypeError("{}.list_objects() requires keyword argument 'bucket'".format(__name__))
        if 'client' is None:
        	raise TypeError("{}.list_objects() requires keyword argument 'client'".format(__name__))

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

    def get_metadata(**kwargs):
        """Gets metadata of a given object key.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            client [REQUIRED]: boto3 session client created by get_session()
            key (str) [REQUIRED]: Name of s3 object key.

        Returns:
            (dict) metadata of given object
        """
        if 'client' not in kwargs:
        	raise KeyError("{}.get_metadata() requires keyword argument 'client'".format(__name__))
        if 'bucket' not in kwargs:
        	raise KeyError("{}.get_metadata() requires keyword argument 'bucket'".format(__name__))
        if 'key' not in kwargs:
        	raise KeyError("{}.get_metadata() requires keyword argument 'key'".format(__name__))

        return client.head_object(Bucket=bucket, Key=key)['Metadata']

    def upload_object(**kwargs):
        """Uploads files to object store.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            client (botocore.client.S3) [REQUIRED]: boto3 client created by get_session
            local_file (str) [REQUIRED]: Filename of local file.
            key (str) [REQUIRED]: Name of s3 object key.
            metadata (dict, str): dict or string representing key/value pairs.

        Returns:
            None
        """
        if 'client' not in kwargs:
        	raise KeyError("{}.get_metadata() requires keyword argument 'client'".format(__name__))
        if 'bucket' not in kwargs:
        	raise KeyError("{}.get_metadata() requires keyword argument 'bucket'".format(__name__))
        if 'local_file' not in kwargs:
        	raise KeyError("{}.get_metadata() requires keyword argument 'local_file'".format(__name__))
        if 'key' not in kwargs:
        	raise KeyError("{}.get_metadata() requires keyword argument 'key'".format(__name__))
        try:
            metadata = kwargs['metadata']
        except KeyError:
            return client.upload_file(local_file, bucket, key)

        meta_dict = {'Metadata' : None}
        if type(metadata) is str:
            # Parse string or check if file exists
             meta_dict['Metadata'] = json.loads(metadata)
        elif type(metadata) is dict:
            #TODO assert it's a flat dict
            meta_dict['Metadata'] = metadata

        return client.upload_file(local_file, bucket, key, ExtraArgs=meta_dict)

    def get_filelist(local_dir, recursive=False, ignore=[]):
        """Returns local filelist.

        Args:
            local_dir (str) [REQUIRED]: local directory to scan
            recursive (bool): whether or not to recursively scan directory.
                              Does not follow symlinks.
            ignore (iterable[str]): strings to ignore.
        """
        if 'local_dir' is None:
        	raise TypeError("{}.get_filelist() requires keyword argument 'local_dir'".format(__name__))

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

    def upload_mult_objects(**kwargs):
        """Uploads files within a directory.

        Uses key from local files.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            local_dir (str) [REQUIRED]: Name of directory to upload
            client (botocore.client.S3) [REQUIRED]: boto3 session client created by get_session()
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
        if 'client' not in kwargs:
            raise KeyError("{}.upload_mult_objects() requires keyword argument 'client'".format(__name__))
        if 'bucket' not in kwargs:
            raise KeyError("{}.upload_mult_objects() requires keyword argument 'bucket'".format(__name__))
        if 'local_dir' not in kwargs:
            raise KeyError("{}.upload_mult_objects() requires keyword argument 'local_dir'".format(__name__))
        try:
            key_prefix = kwargs['key_prefix']
        except KeyError:
            key_prefix = ""
        try:
            recursive = kwargs['recursive']
        except KeyError:
            recursive = False
        try:
            ignore = kwargs['ignore']
        except KeyError:
            ignore = []
        try:
            dry_run = kwargs['dry_run']
        except KeyError:
            dry_run = False
        try:
            metadata = kwargs['metadata']
        except KeyError:
            metadata = None

        filelist = _get_filelist(local_dir=local_dir, recursive=recursive, ignore=ignore)
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

    def interpret_metadata_str(metadata):
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

    def delete(**kwargs):
        """Deletes Key from given bucket.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            key (str) [REQUIRED]: Name of s3 object key.
            client (botocore.client.S3) [REQUIRED]: boto3 session client created by get_session()

        Returns:
            None
        """
        if 'client' not in kwargs:
        	raise KeyError("{}.delete() requires keyword argument 'client'".format(__name__))
        if 'bucket' not in kwargs:
        	raise KeyError("{}.delete() requires keyword argument 'bucket'".format(__name__))
        if 'key' not in kwargs:
        	raise KeyError("{}.delete() requires keyword argument 'key'".format(__name__))

        return client.delete_object(Bucket=bucket, Key=key)

    def get_object(**kwargs):
        """Get's object from store.

        Writes to local dir

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            key (str) [REQUIRED]: Name of s3 object key.
            client (botocore.client.S3) [REQUIRED]: boto3 session client created by get_session()
            write_dir (str): directory to write file to.

        Returns:
            None
        """
        if 'client' not in kwargs:
        	raise KeyError("{}.get_object() requires keyword argument 'client'".format(__name__))
        if 'bucket' not in kwargs:
        	raise KeyError("{}.get_object() requires keyword argument 'bucket'".format(__name__))
        if 'key' not in kwargs:
        	raise KeyError("{}.get_object() requires keyword argument 'key'".format(__name__))
        try:
            write_dir = kwargs['write_dir']
        except KeyError:
            write_dir = './'

        local_filename = os.path.basename(key)
        client.download_file(bucket, key, local_filename)

    def delete_mult(**kwargs):
        """delete objects where keys match regex.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            client (botocore.client.S3) [REQUIRED]: boto3 session client created by get_session()
            obj_regex (str): Regular expression to match against
            dry_run (bool): Print delete command as a sanity check.  No action taken if True.
        """
        if 'client' not in kwargs:
        	raise KeyError("{}.delete_mult() requires keyword argument 'client'".format(__name__))
        if 'bucket' not in kwargs:
        	raise KeyError("{}.delete_mult() requires keyword argument 'bucket'".format(__name__))
        try:
            obj_regex = kwargs['obj_regex']
        except KeyError:
            obj_regex = None
        try:
            dry_run = kwargs['dry_run']
        except KeyError:
            dry_run = False

        all_keys = list_objects(bucket=bucket, client=client, regex=obj_regex, keys_only=True)
        matching_keys = []
        for key in all_objs:
            if dry_run:
                print('Deleting:' + bucket + '/' + key)
            else:
                delete(bucket, key)

    def search_metadata(**kwargs):
        """Search metadata. Narrow search using regex for keys.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            client (botocore.client.S3) [REQUIRED]: boto3 session client created by get_session()
            obj_regex (str): Regular expression to narrow search
            metadata_key (str): dict key of metadata to search

        Returns:
            (list): keys that match
        """
        if 'client' not in kwargs:
        	raise KeyError("{}.search_metadata() requires keyword argument 'client'".format(__name__))
        if 'bucket' not in kwargs:
        	raise KeyError("{}.search_metadata() requires keyword argument 'bucket'".format(__name__))
        try:
            obj_regex = kwargs['obj_regex']
        except KeyError:
            obj_regex = None
        try:
            metadata_key = kwargs['metadata_key']
        except KeyError:
            metadata_key = None

        all_keys = list_objects(bucket, regex=obj_regex, keys_only=True)
        matching_keys = []
        for key in all_keys:
            return_dict = get_metadata(bucket, key)
            if metadata_key in return_dict.keys():
                matching_keys.append(key)

        return matching_keys


def exit_session(error):
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

