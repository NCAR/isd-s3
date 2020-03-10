import logging

# configure logging for a library, per python best practices:
# https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
# NB: this won't work on py2.6 because `logging.NullHandler` wasn't added yet
logging.getLogger("rda_s3").addHandler(logging.NullHandler())
