import sys
import logging
from logging.handlers import RotatingFileHandler

from isd_s3 import isd_s3

__all__ = (
    "isd_s3"
)

"""
    Configure logging for a library, per Python best practices:
    https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
    NB: this won't work in Python older than v3.1 because `logging.NullHandler`
    wasn't added yet.

    NB #2: External apps should delete the logging handler added below and
    instead uncomment the first line that adds the NullHandler.  See note
    in above link for more information.
"""

# External apps and developers: use this line and add a custom logging handler
# in your own code:
# logging.getLogger("rda_s3").addHandler(logging.NullHandler())

# External apps and developers: delete or uncomment the following logging configuration:
LOGPATH = '/glade/u/home/rdadata/dssdb/log'
LOGFILE = 'isd-s3.log'
logging.getLogger("isd_s3")
try:
    handler = RotatingFileHandler(LOGPATH+'/'+LOGFILE,maxBytes=2000000,backupCount=1)
except:
    handler = logging.StreamHandler(sys.stdout) # Go stdout if logpath not defined
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger("isd_s3").addHandler(handler)
