#!/usr/bin/env python
import sys
import os

from isd_s3.__main__ import main, read_json_from_stdin, flatten_dict
from isd_s3 import config
import logging

if __name__ == '__main__':
    # For defaults use:
    # config.configure_logging(**config.get_default_log_config())
    rda_config = '/glade/u/home/rdadata/.aws/isd_s3_config'
    config.configure_logging_from_file(rda_config)
    config.configure_environment_from_file(rda_config)
    from_pipe = not os.isatty(sys.stdin.fileno())
    if from_pipe:
        json_input = read_json_from_stdin()
        if isinstance(json_input, list):
            for command_json in json_input:
                main(*flatten_dict(command_json))
        else:
            main(*flatten_dict(json_input))
    else:
        main(*sys.argv[1:])
