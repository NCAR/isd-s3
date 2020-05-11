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

def get_defult_log_config():
    """Returns default logging configuration."""

    default_config = {
            'logpath' : './',
            'logfile' : 'isd_s3.log',
            'loglevel' : 'info',
            'maxbytes' : 2000000,
            'backupcount' : 1,
            'logfmt' : '%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s',
            'dbgfile' : 'isd_s3.dbg',
            'dbgfmt' : logging.BASIC_FORMAT
          }
    return default_config

def configure_logging_from_file(ini_file=None):
    """Configure logging from .ini file

    Logging can be configured in the configuration file 'isd_s3.ini' as follows:

    [logging]
    logpath = /path/to/log/file
    logfile = isd-s3.log
    dbgfile = isd-s3.dbg
    loglevel = info
    maxbytes = 2000000
    backupcount = 1
    logfmt = %(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s
    dbgfmt = %(asctime)s - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s

    Default behavior is to send logging output to stdout if logging is not configured as
    above.
    """
    if ini_file is None:
        home = str(Path.home())
        ini_file = os.path.join(home,'.aws','filename.ini')
    _cfg = read_config_parser(ini_file)

    default_config = get_defult_log_config()
    _cfg['DEFAULT'] = default_config # Apply defaults

    LOG_SECTION = 'logging'

    logpath = _cfg.get(LOG_SECTION, 'logpath')
    level = _cfg.get(LOG_SECTION, 'loglevel')
    dbgfile = _cfg.get(LOG_SECTION, 'dbgfile')
    dbgfmt = _cfg.get(LOG_SECTION, 'dbgfmt')
    logfile = _cfg.get(LOG_SECTION, 'logfile')
    maxbytes = int(_cfg.get(LOG_SECTION, 'maxbytes'))
    backupcount = int(_cfg.get(LOG_SECTION, 'backupcount'))
    logfmt = _cfg.get(LOG_SECTION, 'logfmt')

    configure_log(logpath, logfile, dbgfile, level, maxbytes, backupcount, logfmt)

def configure_logging(logpath, logfile, dbgfile, loglevel, maxbytes, backupcount, logfmt):
    """Configure logging."""

    # Set logging level.
    LEVELS = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
            }
    level = LEVELS.get(loglevel, logging.INFO)
    logger.setLevel(level)

    # Set up file and log format.
    try:
        if (logger.level == logging.DEBUG):
            # Configure debug logger if in DEBUG mode.
            logging.basicConfig(filename=logpath+'/'+dbgfile,
                                format=dbgfmt,
                                level=level)
        else:
            # Set up standard log file handler.
            handler = RotatingFileHandler(logpath+'/'+logfile,
                                          maxBytes=maxbytes,
                                          backupCount=backupcount)
            formatter = logging.Formatter(logfmt)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    except:
        # Send all warnings and error messages to stdout if above throws an exception.
        logging.basicConfig(level=logging.INFO)
        logger.warning("Logging configuration failed.  All warnings and error messages will be directed to stdout.")

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

    default_config = get_defult_environment()
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

def set_credentials_file(credentials):
    if credentials is not None:
        os.environ[AWS_SHARED_CREDENTIALS_FILE] = credentials
        logger.info('Credentials file set to {}'.format(credentials))

def set_default_bucket(default_bucket):
    if default_bucket is not None:
        os.environ[ISD_S3_DEFAULT_BUCKET] = credentials
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
