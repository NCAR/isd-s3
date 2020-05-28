#!/usr/bin/env python

import sys
import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
try:
    from configparser import ConfigParser, ExtendedInterpolation
except:
    from ConfigParser import ConfigParser, ExtendedInterpolation

logger = logging.getLogger(__name__)

ISD_S3_DEFAULT_BUCKET = 'ISD_S3_DEFAULT_BUCKET'
AWS_SHARED_CREDENTIALS_FILE = 'AWS_SHARED_CREDENTIALS_FILE'
S3_URL = 'S3_URL'

def read_config_parser(filename):
    """Get configuration parser."""
    # Load RDA configuration
    try:
        _cfg = ConfigParser(interpolation=ExtendedInterpolation())
        _cfg.read(filename, encoding='utf-8')
        return _cfg
    except:
        logger.warning('Configuration file *.ini not found')
        raise


def get_default_environment():
    """Returns the default environment as a dict"""
    return {
            's3_url' : 'https://stratus.ucar.edu',
            'credentials' : None, # defaults to ~/.aws/credentials
            'bucket' : None
          }

def configure_environment_from_file(ini_file=None):
    """Configures the environment given an ini file.
    If ini_file is not provided. Uses file, ~/aws/isd_s3.ini

    Args:
        ini_file (str): ini configuration file.
    """
    if ini_file is None:
        home = str(Path.home())
        ini_file = os.path.join(home,'.aws','isd_s3.ini')
    _cfg = read_config_parser(ini_file)

    default_config = get_default_environment()
    s3_url = _cfg.get('default', 's3_url')
    credentials = _cfg.get('default', 'credentials')
    default_bucket = _cfg.get('default', 'bucket')

    configure_environment(s3_url, credentials, default_bucket)


def configure_environment(s3_url, credentials, default_bucket):
    """ Set environment variables for S3 configuration """

    set_s3_url(s3_url)
    set_credentials_file(credentials)
    set_default_bucket(default_bucket)

def set_s3_url(s3_url):
    if s3_url is not None:
        os.environ[S3_URL] = s3_url
        logger.info('S3_URL is {}.'.format(s3_url))
    else:
        os.environ[S3_URL] = get_default_environment()['s3_url']

def set_credentials_file(credentials):
    if credentials is not None:
        os.environ[AWS_SHARED_CREDENTIALS_FILE] = credentials
        logger.info('Credentials file set to {}'.format(credentials))

def set_default_bucket(default_bucket):
    if default_bucket is not None:
        os.environ[ISD_S3_DEFAULT_BUCKET] = default_bucket
        logger.info('Default bucket set to {}'.format(default_bucket))

def get_s3_url():
    if S3_URL in os.environ:
        return os.environ[S3_URL]
    return None

def get_credentials_file():
    if AWS_SHARED_CREDENTIALS_FILE in os.environ:
        return os.environ[AWS_SHARED_CREDENTIALS_FILE]
    return None

def get_default_bucket():
    if ISD_S3_DEFAULT_BUCKET in os.environ:
        return os.environ[ISD_S3_DEFAULT_BUCKET]
    return None
