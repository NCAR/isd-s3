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
import mimetypes

from boto3.s3.transfer import TransferConfig
if __package__ is None or __package__ == "":
    import config
else:
    from . import config

logger = logging.getLogger(__name__)

class Session(object):

    def __init__(self, endpoint_url=None, credentials_loc=None, default_bucket=None, verify=True):
        """Session constructor

        Args:
            endpoint_url (str): The s3 url to connect to.
            credentials_loc (str): location of the credentials file.
                                   (default: ~/.aws/credentials)
            default_bucket (str): bucket to use if not specified explicitly.
        """


        config.configure_environment(endpoint_url, credentials_loc, default_bucket)
        self.client = self.get_session(verify=verify)

    def get_session(self, endpoint_url=None, verify=True):
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
        if endpoint_url is None:
            s3_url = config.get_s3_url()
            if s3_url is None:
                s3_url = config.get_default_environment()['s3_url']
            endpoint_url = s3_url


        session = boto3.session.Session()
        s3_protocol_identifier = 's3://'
        if endpoint_url.startswith(s3_protocol_identifier):
            bucket = endpoint_url.split(s3_protocol_identifier)[1]
            config.set_default_bucket(bucket)
            return session.client(
                    service_name='s3',
                    verify=verify
                    )

        return session.client(
                service_name='s3',
                endpoint_url=endpoint_url,
                verify=verify
                )

    def list_buckets(self, buckets_only=False):
        """Lists all buckets.

        Args:
            buckets_only (bool): Only return bucket names. Default False.

        Returns:
            (list) : list of buckets.
        """
        response = self.client.list_buckets()['Buckets']
        if buckets_only:
            return list(map(lambda x: x['Name'], response))
        return response

    def get_bucket(self, bucket):
        """Returns default bucket if bucket not defined.
        Otherwise raises exception.
        """
        if bucket is not None:
            return bucket

        env_bucket = config.get_default_bucket()
        if env_bucket is None:
            error_msg = 'Default bucket not specified, or available in "' + \
                     config.ISD_S3_DEFAULT_BUCKET + '" environment variable'
            logger.error(error_msg)
            raise ISD_S3_Exception(error_msg)
        return env_bucket


    def directory_list(self, bucket=None, prefix="", ls=False, keys_only=False):
        """Lists directories using a prefix, similar to POSIX ls

        Args:
            bucket (str): Name of s3 bucket.
            prefix (str): Prefix from which to filter.
            ls (bool): Defaut False
            keys_only (bool): Only return the keys.  Default False.
        """
        bucket = self.get_bucket(bucket)
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')

        if 'CommonPrefixes' in response:
            return list(map(lambda x: x['Prefix'], response['CommonPrefixes']))
        if 'Contents' in response:
            return list(map(lambda x: x['Key'], response['Contents']))
        return [] # Can't find anything



    def disk_usage(bucket=None, prefix="",regex=None,block_size='1MB'):
        """Returns the disk usage for a set of objects.

        Args:
            bucket (str) [REQUIRED]: Name of s3 bucket.
            prefix (str): Prefix from which to filter.
            regex (str): regex string.  Default None
            block_size (str): block size

        Returns (dict): disk usage of objects>

        """
        bucket = self.get_bucket(bucket)

        contents = self.list_objects(bucket, prefix, regex=regex)
        total = 0
        divisor = parse_block_size(block_size)
        for _object in contents:
            total += _object['Size'] / divisor
        return {'disk_usage':total,'units':block_size}

    def list_objects(self, bucket=None, prefix="", ls=False, keys_only=False, regex=None):
        """Lists objects from a bucket, optionally matching _prefix.

        prefix should be heavily preferred.

        Args:
            bucket (str): Name of s3 bucket.
            prefix (str): Prefix from which to filter.
            ls (bool): Get 'directories'.
            keys_only (bool): Only return the keys.
            regex (str): regex string

        Returns:
            (list) : list of objects in given bucket
        """
        bucket = self.get_bucket(bucket)

        if ls:
            #if len(prefix) > 0 and prefix[-1] != '/':
            #    prefix += '/'
            return self.directory_list(bucket, prefix, keys_only)

        contents = []

        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            return []
        contents.extend(response['Contents'])
        while response['IsTruncated']:
            response = self.client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=response['NextContinuationToken'])
            contents.extend(response['Contents'])
        if regex is not None:
            contents = self.regex_filter(contents, regex)
        if keys_only:
            return list(map(lambda x: x['Key'], contents))

        return contents

    def regex_filter(self, contents, regex_str, exclude=0):
        """Filters contents using regular expression.

        Args:
            contents (list): response 'Contents' objects
            regex_str (str): regular expression string
            exclude (int): number of characters to exclude from start

        Returns:
            (list) Contents objects.

        """
        filtered_objects = []
        regex = re.compile(regex_str)
        for _object in contents:
            match_against = _object['Key']
            if exclude > 0:
                match_against = match_against[exclude:]
            match = regex.match(match_against)
            if match is not None:
                filtered_objects.append(_object)

        return filtered_objects

    def get_metadata(self, key, bucket=None):
        """Gets metadata of a given object key.

        Args:
            bucket (str) : Name of s3 bucket.
            key (str) : Name of s3 object key.

        Returns:
            (dict) metadata of given object
        """
        bucket = self.get_bucket(bucket)

        return self.client.head_object(Bucket=bucket, Key=key)#['Metadata']

    def replace_metadata(self, key, bucket=None, metadata=None):
        """Copies files to object store.

        Args:
            key (str): key of object to be replaced.
            bucket (str) : Name of s3 bucket.
            metadata (dict, str): dict or string representing key/value pairs.

        Returns:
            None
        """
        bucket = self.get_bucket(bucket)
        return self.copy_object(key, bucket, key, bucket, metadata)


    def move_object(self, source_key, dest_key, source_bucket=None, dest_bucket=None, metadata=None, dry_run=False):
        """Moves object to new key. This will overwrite an object the new key already exists.


        Args:
            source_key (str): key of object or prefix to be copied.
            dest_key (str): Name of new s3 object key.
            source_bucket (str): bucket of key
            dest_bucket (str) : Name of s3 bucket.
            metadata (dict, str): dict or string representing key/value pairs.
            dry_run (bool): Do not execute, but print expected results.

        Returns:
            None
        """
        source_bucket = self.get_bucket(source_bucket)
        if dest_bucket is None:
            dest_bucket = source_bucket
        keys = self.list_objects(prefix=source_key, bucket=source_bucket, keys_only=True)
        if len(keys) == 0:
            raise ValueError(f'key {source_key} does not exist')
        if len(keys) >= 1:
            old_prefix = source_key
            new_prefix = dest_key
            for k in keys:
                # Remove old 'directory' and replace with new
                new_key = new_prefix + k.replace(old_prefix, '')
                if dry_run:
                    print(f'copying {source_bucket}/{k} to {dest_bucket}/{new_key}')
                    continue
                self.copy_object(k, new_key, source_bucket=source_bucket, dest_bucket=dest_bucket, metadata=metadata)
                self.delete([k], bucket=source_bucket)
            return None

        if dry_run:
            print(f'copying {source_bucket}/{source_key} to {dest_bucket}/{dest_key}')
            return None
        self.copy_object(source_key, dest_key, source_bucket=source_bucket, dest_bucket=dest_bucket, metadata=metadata)
        self.delete([source_key], bucket=source_bucket)
        return None

    def copy_object(self, source_key, dest_key, source_bucket=None, dest_bucket=None, metadata=None):
        """Copies objects to new key or bucket.

        Args:
            source_key (str): key of object to be copied.
            dest_key (str): Name of new s3 object key.
            source_bucket (str): bucket of key
            dest_bucket (str) : Name of s3 bucket.
            metadata (dict, str): dict or string representing key/value pairs.

        Returns:
            None
        """
        source_bucket = self.get_bucket(source_bucket)
        if dest_bucket is None:
            dest_bucket = source_bucket

        if metadata is None:
            return self.client.copy_object(Key=dest_key, Bucket=dest_bucket,
                    CopySource={"Bucket": source_bucket, "Key": source_key})

        meta_dict = {'Metadata' : None}
        if isinstance(metadata, str):
            # Parse string or check if file exists
             meta_dict['Metadata'] = json.loads(metadata)
        elif isinstance(metadata, dict):
            #TODO assert it's a flat dict
            meta_dict['Metadata'] = metadata

            return self.client.copy_object(Key=dest_key, Bucket=dest_bucket,
                    CopySource={"Bucket": source_bucket, "Key": source_key},
                    Metadata=meta_dict,
                    MetadataDirective="REPLACE")

    def add_required_metadata(self, _dict):
        """Adds required metadata to dict.

        Args:
            _dict (dict) : metadata dict

        Returns:
            None (modifies _dict)
        """
        if _dict is None:
            return
        if "uploading_account" not in _dict:
            try:
                # Failure can occur when running on scheduling system
                username = os.getlogin()
                _dict["uploading_account"] = username
            except:
                logging.warning('Cannot access username')
        if "institution" not in _dict:
            _dict["institution"] = "NCAR"


    def upload_object(self, local_file, key, metadata=None, bucket=None, md5=False, verify=True):
        """Uploads files to object store.

        Args:
            local_file (str): Filename of local file.
            key (str): Name of s3 object key.
            metadata (dict, str): dict or string representing key/value pairs.
            bucket (str) : Name of s3 bucket.

        Returns:
            None
        """
        bucket = self.get_bucket(bucket)
        #if metadata is None:
        #    return self.client.upload_file(local_file, bucket, key)

        meta_dict = {'Metadata' : {}}
        if isinstance(metadata, str):
            # Parse string or check if file exists
            try:
                meta_dict['Metadata'] = json.loads(metadata)
            except:
                pass
        elif isinstance(metadata, dict):
            #TODO assert it's a flat dict
            meta_dict['Metadata'] = metadata
        self.add_required_metadata(meta_dict['Metadata'])

        if md5:
            meta_dict['Metadata']['Content-MD5'] = get_md5sum(local_file)
            #meta_dict['ContentMD5'] = get_md5sum(local_file)
        trans_config = TransferConfig(
                use_threads=True,
                max_concurrency=10,
                multipart_threshold=1024*1024*25,
                multipart_chunksize=1024*1024*25)

        content_type = get_content_type(local_file)
        if content_type is not None:
            meta_dict['ContentType'] = content_type
            #meta_dict['ACL'] = "public-read"

        success = False
        etag = calculate_s3_etag(local_file)
        retry = 0
        max_retries = 4
        while not success and retry < max_retries:
            ret = self.client.upload_file(local_file, bucket, key, ExtraArgs=meta_dict, Config=trans_config)
            if verify:
                meta = self.get_metadata(key, bucket=bucket)
                if etag == meta['ETag']:
                    success = True
                else:
                    retry += 1
                    logging.info('Etag doesn\'t match. Retrying')
                    print(etag)
                    print(meta['ETag'])
        if retry == max_retries:
            raise ISD_S3_Exception('ETag verification failed on upload')

        return ret

    def get_filelist(self, local_dir, recursive=False, ignore=[]):
        """Returns local filelist.

        Args:
            local_dir (str) [REQUIRED]: local directory to scan
            recursive (bool): whether to recursively scan directory.
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

    def upload_mult_objects(self, local_dir, key_prefix=None, bucket=None, recursive=False, ignore=[], metadata=None, dry_run=False):
        """Uploads files within a directory.

        Uses key from local files.

        Args:
            bucket (str): Name of s3 bucket.
            local_dir (str) [REQUIRED]: Name of directory to upload
            key_prefix (str): string to prepend to key.
                example: If file is 'test/file.txt' and prefix is 'mydataset/'
                         then, full key would be 'mydataset/test/file.txt'
            recursive (bool): Recursively search directory,
            ignore (iterable[str]): does not upload if string matches
            metadata (func or str): If func, execute giving filename as argument.
                                    Expects metadata return code.
                                    If json str, all objects will have this placed in it.
                                    If location of script, calls script and captures output as
                                    the value of metadata.

        Returns:
            None

        """
        bucket = self.get_bucket(bucket)

        #if local_dir[-1] == '/':
        #    local_dir = local_dir[:-1]
        if local_dir[-1] != '/':
            local_dir = '/'

        junk_path = os.path.dirname(local_dir)
        if junk_path != '':
            junk_path += '/'

        filelist = self.get_filelist(local_dir=local_dir, recursive=recursive, ignore=ignore)
        if metadata is not None:
            func = self._interpret_metadata_str(metadata)
        cpus = multiprocessing.cpu_count()
        for _file in filelist:

            key_without_preceding_path = _file.replace(junk_path,'')
            key = key_prefix + key_without_preceding_path

            metadata_str = None
            print(_file)
            if metadata is not None:
                metadata_str = func(_file)

            if dry_run:
                print('(Dry Run) Uploading: '+_file+" to "+bucket+'/'+key)
            else:
                try:
                    p = multiprocessing.Process(
                            target=self.upload_object,
                            args=(_file, key,metadata_str, bucket ))
                    p.start()
                except:
                    self.upload_object(_file,key,metadata,bucket)


    def interpret_metadata_str(self, metadata):
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


    def get_object(self, key, bucket=None, local_dir='./', local_filename=None):
        """Get's object from store.

        Writes to local dir

        Args:
            key (str) [REQUIRED]: Name of s3 object key.
            bucket (str): Name of s3 bucket.
            write_dir (str): directory to write file to.

        Returns:
            dict : successful or not
        """
        if local_filename is None:
            local_filename = os.path.basename(key)
        os.path.join(local_dir, local_filename)
        self.client.download_file(bucket, key, local_filename)
        return {'result' : 'successful'}

    def delete(self, keys=[], bucket=None, dry_run=False):
        """Deletes Key from given bucket.

        Args:
            key (str) [REQUIRED]: Name of s3 object key.
            dry_run (bool): Print delete command as a sanity check.  No action taken if True.

        Returns:
            None
        """
        if isinstance(keys,str):
            keys=[keys]
        assert len(keys) > 0
        bucket = self.get_bucket(bucket)
        for key in keys:
            if dry_run:
                logging.info('deleting ' + key)
                print('deleting ' + key)
            else:
                self.client.delete_object(Bucket=bucket, Key=key)

    def delete_mult(self, bucket=None, prefix="", obj_regex=None, dry_run=False, recursive=False):
        """Delete objects where keys match regex or prefix.

        Args:
            bucket (str) : Name of s3 bucket.
        """
        bucket = self.get_bucket(bucket)
        if recursive:
            assert obj_regex is None
            all_keys = self.list_objects(bucket=bucket, regex=obj_regex, keys_only=True, prefix=prefix)
        else:
            all_objects = self.list_objects(bucket=bucket, regex=obj_regex, prefix=prefix)
            regex = '^[^/]+$'
            all_objects = self.regex_filter(all_objects, regex, exclude= len(prefix))
            all_keys = list(map(lambda x: x['Key'], all_objects))

        self.delete(bucket=bucket, keys=all_keys, dry_run=dry_run)

    def search_metadata(self, bucket=None, obj_regex=None, metadata_key=None):
        """Search metadata. Narrow search using regex for keys.

        Args:
            bucket (str): Name of s3 bucket.
            obj_regex (str): Regular expression to narrow search
            metadata_key (str): dict key of metadata to search

        Returns:
            (list): keys that match
        """
        bucket = self.get_bucket(bucket)

        all_keys = self.list_objects(bucket, regex=obj_regex, keys_only=True)
        matching_keys = []
        for key in all_keys:
            return_dict = self.get_metadata(bucket, key)
            if metadata_key in return_dict.keys():
                matching_keys.append(key)

        return matching_keys

    def __str__(self):
        mem_adr = super.__str__(self)
        return mem_adr + "\nconfig\n-----\n" + \
                "endpoint: " + str(config.get_s3_url()) + "\n" + \
                "default bucket: " + str(config.get_default_bucket())


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

def calculate_s3_etag(file_path, chunk_size=1024*1024*25):
    import hashlib
    md5s = []

    with open(file_path, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))

    if len(md5s) == 1:
        return '"{}"'.format(md5s[0].hexdigest())

    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '"{}-{}"'.format(digests_md5.hexdigest(), len(md5s))

def get_md5sum(local_file):
    import hashlib
    content_md5 = hashlib.md5(open(local_file,'rb').read()).hexdigest()
    return content_md5

def guess_content_type(filename):
    """Based on the filename, guess content-type.
    """
    pass

class ISD_S3_Exception(Exception):
    pass


def get_content_type(filename):
    """Get MIME type based on filename"""
    return mimetypes.MimeTypes().guess_type(filename)[0]
