#!/usr/bin/env python
"""
Test rda_s3.py
"""
import inspect
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..')
print(sys.path)

from rda_s3 import rda_s3

def passed():
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    print('Passed ', calframe[1][3])

def test_argparse():
    passed()

def test_list_objects():
    passed()

def test_list_buckets():
    passed()

test_argparse()
test_list_objects()
test_list_buckets()


