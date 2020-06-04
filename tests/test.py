#!/usr/bin/env python3
"""
Test isd_s3.py
"""
import sys
import os
import inspect
import pdb
sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..')
from isd_s3 import isd_s3
from isd_s3 import __main__ as main

bucket = 'rda-test-rpconroy'
session = isd_s3.Session(default_bucket='rda-test-rpconroy')


def passed():
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    print('Passed ', calframe[1][3])

def test_argparse():
    parser = main._get_parser()
    args = ['lb', '-bo']
    # Test optional args
    args = parser.parse_args(args)
    args = ['-np', 'lb', '-bo']
    args = parser.parse_args(args)
    args = ['--noprint', 'lb', '-bo']
    args = parser.parse_args(args)
    args = ['--use_local_config', '-pp', 'lb', '-bo']
    args = parser.parse_args(args)
    passed()

def test_upload():
    test_file = 'test.txt'
    with open(test_file, 'w') as fh:
        fh.write('this is a test')
    ret = main.main('-np', 'ul', '-b', bucket, '-lf', test_file, '-k', 'test.txt')
    #ret = main.main('-np', 'ul', '-b', bucket, '-lf', test_file, '-k', 'test.txt')
    os.remove(test_file)
    passed()

def test_list_objects():
    ret = main.main('-np', 'lo', '-b', bucket, 'test', '-ko')
    assert 'test.txt' in ret
    ret = main.main('-np', 'lo', '-b', bucket, 'test.txt')
    assert len(ret) == 1
    assert ret[0]['Size'] == 14
    passed()

def test_list_buckets():
    ret = main.main('-np', 'lb', '-bo')
    assert bucket in ret
    passed()

# Run functions that start with 'test'
funcs = list(filter(lambda x: x[:4] == 'test', dir()))
self = sys.modules[__name__]
for func_str in funcs:
    func = getattr(self, func_str)
    func()


