import sys
import logging

from isd_s3 import isd_s3

__all__ = (
    "isd_s3",
    "config"
)

__version__ = "1.1.2"

"""
    Configure logging for a library, per Python best practices:
    https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
    NB: this won't work in Python older than v3.1 because `logging.NullHandler`
    wasn't added yet.
"""
logging.getLogger("isd_s3").addHandler(logging.NullHandler())
