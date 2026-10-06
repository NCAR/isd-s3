#!/usr/bin/env python3
"""Integration tests for isd_s3. These talk to a REAL object store.

Run from the repository root on a machine that can reach the store, with
credentials configured (e.g. ~/.aws/credentials):

    ISD_S3_TEST_BUCKET=<bucket you may write to> python -m unittest -v tests.test_integration

Optional environment variables:
    ISD_S3_TEST_URL   endpoint to use (default: the isd_s3 default endpoint)
    ISD_S3_TEST_CREDS credentials file (default: ~/.aws/credentials)

Do not use `unittest discover` on this directory: the older tests/test.py runs
its tests at import time.

Every test works under a unique prefix, `isd_s3_test/<uuid>/`, which is
deleted at the end of the run. Nothing outside that prefix is touched.
"""
import os
import shutil
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/..')
from isd_s3 import isd_s3
from isd_s3 import __main__ as cli

BUCKET = os.environ.get('ISD_S3_TEST_BUCKET')
URL = os.environ.get('ISD_S3_TEST_URL')
CREDS = os.environ.get('ISD_S3_TEST_CREDS')


@unittest.skipUnless(BUCKET, 'set ISD_S3_TEST_BUCKET to run integration tests')
class IntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.prefix = 'isd_s3_test/{}/'.format(uuid.uuid4().hex)
        cls.session = isd_s3.Session(endpoint_url=URL, credentials_loc=CREDS,
                                     default_bucket=BUCKET)

    @classmethod
    def tearDownClass(cls):
        cls.session.delete_mult(bucket=BUCKET, prefix=cls.prefix, recursive=True)

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    # -- helpers ---------------------------------------------------------
    def key(self, name):
        return self.prefix + name

    def write(self, relpath, content='hello'):
        path = os.path.join(self.tmp, relpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as fh:
            fh.write(content)
        return path

    def put(self, name, content='hello', metadata=None):
        path = self.write('put_' + uuid.uuid4().hex, content)
        self.session.upload_object(path, self.key(name), metadata=metadata)
        return path

    def keys(self, name=''):
        return self.session.list_objects(bucket=BUCKET, prefix=self.key(name), keys_only=True)

    # -- basic operations -----------------------------------------------
    def test_list_buckets(self):
        self.assertIn(BUCKET, self.session.list_buckets(buckets_only=True))
        self.assertIn(BUCKET, [b['Name'] for b in self.session.list_buckets()])

    def test_upload_list_get_delete_roundtrip(self):
        self.put('rt/file.txt', 'round trip')
        self.assertEqual(self.keys('rt/'), [self.key('rt/file.txt')])

        objs = self.session.list_objects(bucket=BUCKET, prefix=self.key('rt/'))
        self.assertEqual(len(objs), 1)
        self.assertEqual(objs[0]['Size'], len('round trip'))

        out = os.path.join(self.tmp, 'out')
        os.makedirs(out)
        self.session.get_object(self.key('rt/file.txt'), local_dir=out)
        with open(os.path.join(out, 'file.txt')) as fh:
            self.assertEqual(fh.read(), 'round trip')

        self.session.delete([self.key('rt/file.txt')])
        self.assertEqual(self.keys('rt/'), [])

    def test_get_object_local_filename(self):
        self.put('rename/file.txt', 'abc')
        self.session.get_object(self.key('rename/file.txt'), local_dir=self.tmp,
                                local_filename='renamed.txt')
        with open(os.path.join(self.tmp, 'renamed.txt')) as fh:
            self.assertEqual(fh.read(), 'abc')

    def test_upload_without_verify(self):
        path = self.write('nv.txt', 'no verify')
        self.session.upload_object(path, self.key('nv/nv.txt'), verify=False)
        self.assertEqual(self.keys('nv/'), [self.key('nv/nv.txt')])

    def test_upload_md5(self):
        path = self.write('md5.txt', 'md5 me')
        self.session.upload_object(path, self.key('md5/md5.txt'), md5=True)
        meta = self.session.get_metadata(self.key('md5/md5.txt'), bucket=BUCKET)
        self.assertEqual(meta['Metadata']['content-md5'], isd_s3.get_md5sum(path))

    def test_list_objects_regex_and_ls(self):
        self.put('rx/a.txt')
        self.put('rx/b.dat')
        self.put('rx/sub/c.txt')
        txt = self.session.list_objects(bucket=BUCKET, prefix=self.key('rx/'),
                                        regex=r'.*\.txt$', keys_only=True)
        self.assertEqual(sorted(txt), [self.key('rx/a.txt'), self.key('rx/sub/c.txt')])
        # directory level listing returns the 'sub/' prefix when one exists
        self.assertEqual(self.session.list_objects(bucket=BUCKET, prefix=self.key('rx/'), ls=True),
                         [self.key('rx/sub/')])

    def test_list_objects_honors_bucket_argument(self):
        self.put('lb/x.txt')
        other = isd_s3.Session(endpoint_url=URL, credentials_loc=CREDS)  # no default bucket
        keys = other.list_objects(bucket=BUCKET, prefix=self.key('lb/'), keys_only=True)
        self.assertEqual(keys, [self.key('lb/x.txt')])

    # -- metadata --------------------------------------------------------
    def test_metadata_on_upload(self):
        self.put('md/m.txt', metadata={'project': 'unit'})
        meta = self.session.get_metadata(self.key('md/m.txt'), bucket=BUCKET)['Metadata']
        self.assertEqual(meta['project'], 'unit')
        self.assertEqual(meta['institution'], 'NCAR')  # added by add_required_metadata

    def test_replace_metadata(self):
        self.put('rm/m.txt', metadata={'color': 'red'})
        self.session.replace_metadata(self.key('rm/m.txt'), bucket=BUCKET,
                                      metadata={'color': 'blue'})
        meta = self.session.get_metadata(self.key('rm/m.txt'), bucket=BUCKET)['Metadata']
        self.assertEqual(meta, {'color': 'blue'})
        self.assertEqual(self.keys('rm/'), [self.key('rm/m.txt')])

    def test_search_metadata(self):
        self.put('sm/has.txt', metadata={'findme': '1'})
        self.put('sm/not.txt')
        found = self.session.search_metadata(bucket=BUCKET, obj_regex=r'.*/sm/.*',
                                             metadata_key='findme')
        self.assertEqual(found, [self.key('sm/has.txt')])

    # -- copy / move -----------------------------------------------------
    def test_copy_object(self):
        self.put('cp/src.txt', 'copy me', metadata={'a': '1'})
        self.session.copy_object(self.key('cp/src.txt'), self.key('cp/dst.txt'))
        self.assertEqual(sorted(self.keys('cp/')),
                         [self.key('cp/dst.txt'), self.key('cp/src.txt')])
        # copy with new metadata
        self.session.copy_object(self.key('cp/src.txt'), self.key('cp/dst2.txt'),
                                 metadata='{"b": "2"}')
        meta = self.session.get_metadata(self.key('cp/dst2.txt'), bucket=BUCKET)['Metadata']
        self.assertEqual(meta, {'b': '2'})

    def test_move_single_object(self):
        self.put('mv1/src.txt')
        self.session.move_object(self.key('mv1/src.txt'), self.key('mv1/dst.txt'))
        self.assertEqual(self.keys('mv1/'), [self.key('mv1/dst.txt')])

    def test_move_prefix(self):
        self.put('mvsrc/a.txt')
        self.put('mvsrc/sub/b.txt')
        self.session.move_object(self.key('mvsrc/'), self.key('mvdst/'), dry_run=True)
        self.assertEqual(len(self.keys('mvsrc/')), 2, 'dry run must not move anything')
        self.session.move_object(self.key('mvsrc/'), self.key('mvdst/'))
        self.assertEqual(self.keys('mvsrc/'), [])
        self.assertEqual(sorted(self.keys('mvdst/')),
                         [self.key('mvdst/a.txt'), self.key('mvdst/sub/b.txt')])

    def test_move_missing_key_raises(self):
        with self.assertRaises(ValueError):
            self.session.move_object(self.key('does/not/exist'), self.key('nowhere'))

    # -- multi-object operations -----------------------------------------
    def make_tree(self):
        tree = os.path.join(self.tmp, 'tree')
        self.write('tree/a.txt', 'aaa')
        self.write('tree/sub/b.txt', 'bbbb')
        self.write('tree/sub/skip.tmp', 'skip')
        return tree

    def test_upload_mult_non_recursive(self):
        tree = self.make_tree()
        self.session.upload_mult_objects(tree, key_prefix=self.key('um1'), bucket=BUCKET)
        self.assertEqual(self.keys('um1/'), [self.key('um1/a.txt')])

    def test_upload_mult_recursive_ignore_and_dry_run(self):
        tree = self.make_tree()
        self.session.upload_mult_objects(tree, key_prefix=self.key('um2'), bucket=BUCKET,
                                         recursive=True, dry_run=True)
        self.assertEqual(self.keys('um2/'), [], 'dry run must not upload')
        self.session.upload_mult_objects(tree, key_prefix=self.key('um2'), bucket=BUCKET,
                                         recursive=True, ignore=['.tmp'],
                                         metadata='{"batch": "x"}')
        self.assertEqual(sorted(self.keys('um2/')),
                         [self.key('um2/a.txt'), self.key('um2/sub/b.txt')])
        meta = self.session.get_metadata(self.key('um2/a.txt'), bucket=BUCKET)['Metadata']
        self.assertEqual(meta['batch'], 'x')

    def test_upload_directory_via_upload_object(self):
        tree = self.make_tree()
        self.session.upload_object(tree, self.key('ud'))
        self.assertEqual(sorted(self.keys('ud/')),
                         [self.key('ud/a.txt'), self.key('ud/sub/b.txt'),
                          self.key('ud/sub/skip.tmp')])

    def test_get_object_recursive(self):
        self.put('gr/a.txt', 'aaa')
        self.put('gr/sub/b.txt', 'bbbb')
        self.put('grother/c.txt', 'ccc')  # shares the 'gr' string prefix; must not be fetched
        out = os.path.join(self.tmp, 'dl')

        self.session.get_object(self.key('gr'), local_dir=out, recursive=True, dry_run=True)
        self.assertFalse(os.path.exists(out), 'dry run must not download')

        result = self.session.get_object(self.key('gr'), local_dir=out, recursive=True)
        self.assertEqual(result['count'], 2)
        with open(os.path.join(out, 'a.txt')) as fh:
            self.assertEqual(fh.read(), 'aaa')
        with open(os.path.join(out, 'sub', 'b.txt')) as fh:
            self.assertEqual(fh.read(), 'bbbb')
        self.assertFalse(os.path.exists(os.path.join(out, 'c.txt')))

    def test_get_object_recursive_missing_prefix_raises(self):
        with self.assertRaises(ValueError):
            self.session.get_object(self.key('nothing/here'), local_dir=self.tmp, recursive=True)

    def test_delete_mult(self):
        self.put('dm/a.txt')
        self.put('dm/sub/b.txt')
        prefix = self.key('dm/')

        self.session.delete_mult(bucket=BUCKET, prefix=prefix, recursive=True, dry_run=True)
        self.assertEqual(len(self.keys('dm/')), 2, 'dry run must not delete')

        # non-recursive only removes the top level
        self.session.delete_mult(bucket=BUCKET, prefix=prefix)
        self.assertEqual(self.keys('dm/'), [self.key('dm/sub/b.txt')])

        self.session.delete_mult(bucket=BUCKET, prefix=prefix, recursive=True)
        self.assertEqual(self.keys('dm/'), [])

    def test_delete_over_batch_size(self):
        # Exercises DeleteObjects batching without needing >1000 real uploads:
        # missing keys are deleted without error in S3.
        fake = [self.key('batch/{}'.format(i)) for i in range(isd_s3.DELETE_BATCH_SIZE + 5)]
        self.session.delete(fake, bucket=BUCKET)

    def test_disk_usage(self):
        self.put('du/a.txt', 'x' * 1000)
        self.put('du/b.txt', 'y' * 3000)
        usage = self.session.disk_usage(bucket=BUCKET, prefix=self.key('du/'), block_size='1KB')
        self.assertAlmostEqual(usage['disk_usage'], 4.0)
        self.assertEqual(usage['units'], '1KB')

    # -- CLI entry point ---------------------------------------------------
    def run_cli(self, *args):
        common = ['-np', '-db', BUCKET]
        if URL:
            common += ['--s3_url', URL]
        if CREDS:
            common += ['-cf', CREDS]
        return cli.main(*(common + list(args)))

    def test_cli_upload_list_get_move_delete(self):
        path = self.write('cli.txt', 'cli test')
        key = self.key('cli/cli.txt')
        self.run_cli('ul', '-lf', path, '-k', key)
        self.assertEqual(self.run_cli('lo', '-b', BUCKET, self.key('cli/'), '-ko'), [key])
        self.assertIn('Metadata', self.run_cli('gm', '-b', BUCKET, '-k', key))

        out = os.path.join(self.tmp, 'cli_out')
        os.makedirs(out)
        self.run_cli('go', '-b', BUCKET, '-k', key, '-ld', out)
        with open(os.path.join(out, 'cli.txt')) as fh:
            self.assertEqual(fh.read(), 'cli test')

        self.run_cli('cp', '-b', BUCKET, '-k', key, '-dk', self.key('cli/copy.txt'))
        self.run_cli('mv', '-b', BUCKET, '-k', self.key('cli/copy.txt'),
                     '-dk', self.key('cli/moved.txt'))
        self.assertEqual(sorted(self.keys('cli/')), [key, self.key('cli/moved.txt')])

        self.run_cli('dl', '-b', BUCKET, key, self.key('cli/moved.txt'))
        self.assertEqual(self.keys('cli/'), [])

    def test_cli_recursive_get_and_du(self):
        self.put('clir/a.txt', 'a' * 2000)
        self.put('clir/sub/b.txt', 'b' * 2000)
        out = os.path.join(self.tmp, 'clir_out')
        self.run_cli('go', '-b', BUCKET, '-k', self.key('clir'), '-r', '-ld', out)
        self.assertTrue(os.path.exists(os.path.join(out, 'sub', 'b.txt')))
        usage = self.run_cli('du', '-b', BUCKET, self.key('clir/'), '-k', '1KB')
        self.assertAlmostEqual(usage['disk_usage'], 4.0)


if __name__ == '__main__':
    unittest.main()
