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

def configure_environment():
    """ Set environment variables for S3 configuration """
    default_config = {
            's3_url' : 'https://stratus.ucar.edu',
            'credentials' : None, # defaults to ~/.aws/credentials
            'bucket' : None
          }

    # S3 URL
    s3_url = _cfg.get('default', 's3_url')
    os.environ['S3_URL'] = s3_url
    logger.info('S3_URL is {}.'.format(s3_url))

    # AWS credentials file
    credentials = _cfg.get('default', 'credentials')
    if credentials is not None:
        os.environ['AWS_SHARED_CREDENTIALS_FILE'] = credentials
        logger.info('Credentials file set to {}'.format(credentials))

    # Default bucket name
    default_bucket = _cfg.get('default', 'bucket')
    if default_bucket is not None:
        logger.warning('Default bucket set to {}'.format(DEFAULT_BUCKET))
        os.environ['ISD_S3_DEFAULT_BUCKET'] = credentials

    return {'endpoint' : s3_url,
            'credentials_file': credentials,
            'default_bucket': default_bucket
            }


if __name__ == '__main__':
    configure_log()
    configure_environment()
    main(*sys.argv[1:])
