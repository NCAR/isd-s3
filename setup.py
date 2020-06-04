import sys
from setuptools import find_packages, setup

import isd_s3

if sys.version_info < (3, 1):
    raise NotImplementedError(
        """\n
##############################################################
# isd-s3 does not support python versions older than 3.1 #
##############################################################"""
    )

with open("README.md", "r") as fh:
	long_description = fh.read()

setup(
    name="ncar-isd-s3",
    version=isd_s3.__version__,  # See semantic versioning at https://semver.org/
    author="NCAR RDA Team",
    author_email="rdahelp@ucar.edu",
    description="NCAR ISD S3 object storage utility",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NCAR/isd-s3",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=[
	    "boto3",
	    "botocore"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers"
    ],
    python_requires='>=3.1',
    entry_points={"console_scripts": ["isd_s3=isd_s3.__main__:main"]}
)
