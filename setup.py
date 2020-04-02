import sys
from setuptools import find_packages, setup

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
    version="0.1.3",  # See semantic versioning at https://semver.org/
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
        "Development Status :: 2 - Pre-Alpha",
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
)
