#!/usr/bin/env python
import sys

from isd_s3.__main__ import main
from isd_s3 import config

if __name__ == '__main__':
    # For defaults use:
    # config.configure_logging(**config.get_default_log_config())
    rda_config = '/glade/u/home/rdadata/.aws/isd_s3_config'
    config.configure_logging_from_file(rda_config)
    config.configure_environment_from_file(rda_config)
    main(*sys.argv[1:])
