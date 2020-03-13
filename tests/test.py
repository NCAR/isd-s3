#!/usr/bin/env python
"""
Test rda_s3.py
"""
import sys
import os
import inspect
import pdb
sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..')
from rda_s3 import rda_s3

def passed():
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    print('Passed ', calframe[1][3])

def test_argparse():
    parser = rda_s3._get_parser()
    args = ['lb', '-bo']
    args = parser.parse_args(args)
    passed()

def test_list_objects():
    ret = rda_s3.main('lo', )
    passed()

def test_list_buckets():
    ret = rda_s3.main('lb', )
    passed()

# Run functions that start with 'test'
funcs = list(filter(lambda x: x[:4] == 'test', dir()))
self = sys.modules[__name__]
for func_str in funcs:
    func = getattr(self, func_str)
    func()


