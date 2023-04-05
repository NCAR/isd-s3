#!/usr/bin/env python
import sys
import os
import pdb

from isd_s3.__main__ import main, read_json_from_stdin, flatten_dict
from isd_s3 import config
from logging.handlers import RotatingFileHandler

from configparser import ConfigParser, ExtendedInterpolation
import logging
logger = logging.getLogger('isd_s3')

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

def get_log_levels():
    LEVELS = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
            }
    return LEVELS

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
        ini_file = os.path.join(home,'.aws','isd_s3.ini')
    _cfg = config.read_config_parser(ini_file)

    default_config = get_defult_log_config()
    _cfg['DEFAULT'] = default_config # Apply defaults

    LOG_SECTION = 'logging'

    logpath = _cfg.get(LOG_SECTION, 'logpath')
    #logpath = '/glade/u/home/rpconroy/repositories/isd-s3'
    level = _cfg.get(LOG_SECTION, 'loglevel')
    dbgfile = _cfg.get(LOG_SECTION, 'dbgfile')
    dbgfmt = _cfg.get(LOG_SECTION, 'dbgfmt')
    logfile = _cfg.get(LOG_SECTION, 'logfile')
    maxbytes = int(_cfg.get(LOG_SECTION, 'maxbytes'))
    backupcount = int(_cfg.get(LOG_SECTION, 'backupcount'))
    logfmt = _cfg.get(LOG_SECTION, 'logfmt')

    configure_logging(logpath, logfile, dbgfile, level, maxbytes, backupcount, logfmt)

def configure_logging(logpath, logfile, dbgfile, loglevel, maxbytes, backupcount, logfmt):
    """Configure logging."""

    # Set logging level.
    LEVELS = get_log_levels()
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
            handler = GroupWriteRotatingFileHandler(logpath+'/'+logfile,
                                          maxBytes=maxbytes,
                                          backupCount=backupcount)
            formatter = logging.Formatter(logfmt)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.info('here')
    except Exception as e:

    #    # Send all warnings and error messages to stdout if above throws an exception.
        logging.basicConfig(level=logging.INFO)
        logger.warning("Logging configuration failed due to '"+str(e)+"'  All warnings and error messages will be directed to stdout.")

class GroupWriteRotatingFileHandler(RotatingFileHandler):    
    def _open(self):
        prevumask=os.umask(0o002)
        #os.fdopen(os.open('/path/to/file', os.O_WRONLY, 0600))
        rtv=logging.handlers.RotatingFileHandler._open(self)
        os.umask(prevumask)
        return rtv

if __name__ == '__main__':
    # For defaults use:
    # config.configure_logging(**config.get_default_log_config())
    rda_config = '/glade/u/home/rdadata/.aws/isd_s3_config'
    configure_logging_from_file(rda_config)
    config.configure_environment_from_file(rda_config)
    from_pipe = not os.isatty(sys.stdin.fileno())
    if len(sys.argv) > 1:
        main(*sys.argv[1:])
    elif from_pipe:
        json_input = read_json_from_stdin()
        if isinstance(json_input, list):
            for command_json in json_input:
                main(*flatten_dict(command_json))
        else:
            main(*flatten_dict(json_input))
    else:
        main(*sys.argv[1:])
