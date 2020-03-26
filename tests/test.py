#!/usr/bin/env python3
"""
Test rda_s3.py
"""
import sys
import os
import inspect
import pdb
sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/..')
from rda_s3 import rda_s3

bucket = 'rda-test-rpconroy'

def passed():
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    print('Passed ', calframe[1][3])

def test_argparse():
    parser = rda_s3._get_parser()
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
    ret = rda_s3.main('-np', 'ul', '-b', bucket, '-lf', test_file, '-k', 'test.txt')
    #ret = rda_s3.main('-np', 'ul', '-b', bucket, '-lf', test_file, '-k', 'test.txt')
    os.remove(test_file)
    passed()

def test_list_objects():
    ret = rda_s3.main('-np', 'lo', '-b', bucket, 'test', '-ko')
    assert 'test.txt' in ret
    ret = rda_s3.main('-np', 'lo', '-b', bucket, 'test.txt')
    assert len(ret) == 1
    assert ret[0]['Size'] == 14
    passed()

def test_list_buckets():
    ret = rda_s3.main('-np', 'lb', '-bo')
    assert bucket in ret
    passed()

# Run functions that start with 'test'
funcs = list(filter(lambda x: x[:4] == 'test', dir()))
self = sys.modules[__name__]
for func_str in funcs:
    func = getattr(self, func_str)
    func()


