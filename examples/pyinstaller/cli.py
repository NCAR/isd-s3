#!/usr/bin/env python
from isd_s3.__main__ import main

import sys
import logging
import os
from logging.handlers import RotatingFileHandler
try:
    from configparser import ConfigParser, ExtendedInterpolation
except:
    from ConfigParser import ConfigParser, ExtendedInterpolation

logger = logging.getLogger(__name__)

# Load RDA configuration
try:
    _cfg = ConfigParser(interpolation=ExtendedInterpolation())
    _cfg.read('/glade/u/home/rdadata/.aws/isd_s3_config', encoding='utf-8')
except:
    logger.warning('Configuration file *.ini not found')

def configure_log():
    """ Configure logging

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

    LEVELS = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
            }

    default_config = {
            'logfile' : 'isd_s3.log',
            'loglevel' : 'info',
            'maxbytes' : 2000000,
            'backupcount' : 1,
            'logfmt' : '%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s',
          }

    _cfg['DEFAULT'] = default_config # Apply defaults

    # Set logging level.
    level = LEVELS.get(_cfg.get('logging', 'loglevel'), logging.INFO)
    logger.setLevel(level)


    # Set up file and log format.
    try:
        LOGPATH = _cfg.get('logging', 'logpath')

        if (logger.level == logging.DEBUG):
            # Configure debug logger if in DEBUG mode.
            default_debug_config = {
                'dbgfile' : 'isd_s3.dbg',
                'dbgfmt' : logging.BASIC_FORMAT
               }
            _cfg['DEFAULT'].update(default_debug_config)
            DBGFILE = _cfg.get('logging', 'dbgfile')
            DBGFMT = _cfg.get('logging', 'dbgfmt')
            logging.basicConfig(filename=LOGPATH+'/'+DBGFILE,
                                format=DBGFMT,
                                level=logging.DEBUG)
        else:
            # Set up standard log file handler.
            LOGFILE = _cfg.get('logging', 'logfile')
            LOGBYTES = int(_cfg.get('logging', 'maxbytes'))
            LOGBACKUP = int(_cfg.get('logging', 'backupcount'))
            LOGFMT = _cfg.get('logging', 'logfmt')
            handler = RotatingFileHandler(LOGPATH+'/'+LOGFILE,
                                          maxBytes=LOGBYTES,
                                          backupCount=LOGBACKUP)
            formatter = logging.Formatter(LOGFMT)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    except:
        # Send all warnings and error messages to stdout if above throws an exception.
        logging.basicConfig(level=logging.INFO)
        logger.warning("Logging configuration not found.  All warnings and error messages will be directed to stdout.")

def configure_environment():
    """ Set environment variables for S3 configuration """

    # S3 URL
    if (_cfg.has_option('default', 's3_url')):
        S3_URL = _cfg.get('default', 's3_url')
        os.environ['S3_URL'] = S3_URL
        logger.info('S3_URL {} successfully read from config file'.format(S3_URL))
    else:
        S3_URL = 'https://stratus.ucar.edu'
        os.environ['S3_URL'] = S3_URL
        logger.warning('Configuration not found for S3_URL.  Setting to default value {}'.format(os.environ['S3_URL']))

    # AWS credentials file
    if (_cfg.has_option('default', 'credentials')):
        credentials = _cfg.get('default', 'credentials')
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = credentials
        logger.info('AWS_SHARED_CREDENTIALS_FILE {} successfully read from config file'.format(os.environ['AWS_SHARED_CREDENTIALS_FILE']))
    else:
        credentials = '/glade/u/home/rdadata/.aws/credentials'
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = credentials
        logger.warning('Configuration not found for S3 credentials file.  Setting to default value {}'.format(credentials))

    # Default bucket name
    if (_cfg.has_option('default', 'bucket')):
        DEFAULT_BUCKET = _cfg.get('default', 'bucket')
    else:
        DEFAULT_BUCKET = 'rda-data'
        logger.warning('Configuration not found for S3 default bucket name.  Setting to default value {}'.format(DEFAULT_BUCKET))

if __name__ == '__main__':
    configure_log()
    configure_environment()
    main(*sys.argv[1:])
