#!/usr/bin/env python
"""
swiftnbd. tests for the common module
Copyright (C) 2012 by Juan J. Martinez <jjm@usebox.net>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import unittest
from hashlib import md5
import errno

class MockConnection(object):
    """
    Mock up for the Swift client.

    Initialised with 8 blocks of 512 bytes.
    """

    block_size = 512
    blocks = dict()

    class ClientException(Exception):
        def __init__(self, http_status=404):
            self.http_status = http_status

    def __init__(self):
        MockConnection.blocks = dict(("disk.part/%.8i" % block_num, '\xff'*MockConnection.block_size) for block_num in range(8))

    @staticmethod
    def block(block_num):
        return MockConnection.blocks["disk.part/%.8i" % block_num]

    @staticmethod
    def Connection(authurl, username, password):
        return MockConnection()

    def get_container(self, container):
        pass

    def get_object(self, container, block_name):
        try:
            return None, MockConnection.blocks[block_name]
        except KeyError:
            raise MockConnection.ClientException()

    def put_object(self, container, block_name, obj):
        data = obj.read()

        assert len(data) == self.block_size, "Data size mismatch"

        MockConnection.blocks[block_name] = data
        return md5(data).hexdigest()

class SwiftBlockFileTestCase(unittest.TestCase):
    """Test the block-split file class."""
    def setUp(self):
        # monkey-patch swiftclient to use out mock up
        import swiftnbd.common as common
        common.client = MockConnection
        from swiftnbd.common import SwiftBlockFile

        # create a disk doubling the actual size of the mock up so we
        # have some uninitialised space to run tests
        self.store = SwiftBlockFile('url', 'user', 'pass', 'container', 512, 16)
        self.store.flush()

    def tearDown(self):
        pass

    def test_read_full_block_content(self):
        self.store.seek(0)
        data = self.store.read(512)
        self.assertEqual(data, '\xff'*512)

    def test_read_full_block_no_content(self):
        self.store.seek(8*512)
        data = self.store.read(512)
        self.assertEqual(data, '\0'*512)

    def test_write_full_block(self):
        self.store.seek(0)
        self.store.write('X' * 512)
        self.assertEqual(MockConnection.block(0), 'X'*512)

        self.store.seek(8*512)
        self.store.write('X' * 512)
        self.assertEqual(MockConnection.block(8), 'X'*512)

    def test_read_partial_block_content(self):
        self.store.seek(0)
        data = self.store.read(256)
        self.assertEqual(data, '\xff'*256)

    def test_read_partial_block_no_content(self):
        self.store.seek(8*512)
        data = self.store.read(256)
        self.assertEqual(data, '\0'*256)

    def test_write_partial_block_content(self):
        self.store.seek(0)
        self.store.write('X' * 256)
        self.assertEqual(MockConnection.block(0), 'X'*256 + '\xff'*256)

    def test_write_partial_block_no_content(self):
        self.store.seek(8*512)
        self.store.write('X' * 256)
        self.assertEqual(MockConnection.block(8), 'X'*256 + '\0'*256)

    def test_read_inter_block_content(self):
        self.store.seek(256)
        data = self.store.read(512)
        self.assertEqual(data, '\xff'*512)

    def test_read_inter_block_no_content(self):
        self.store.seek(8*512 + 256)
        data = self.store.read(512)
        self.assertEqual(data, '\0'*512)

    def test_read_inter_block_content_and_no_content(self):
        self.store.seek(8*512 - 256)
        data = self.store.read(512)
        self.assertEqual(data, '\xff'*256 +  '\0'*256)

    def test_write_inter_block_content(self):
        self.store.seek(256)
        self.store.write('X' * 512)
        self.assertEqual(MockConnection.block(0), '\xff'*256 + 'X'*256)
        self.assertEqual(MockConnection.block(1), 'X'*256 + '\xff'*256)

    def test_write_inter_block_no_content(self):
        self.store.seek(8*512 + 256)
        self.store.write('X' * 512)
        self.assertEqual(MockConnection.block(8), '\0'*256 + 'X'*256)
        self.assertEqual(MockConnection.block(9), 'X'*256 + '\0'*256)

    def test_write_inter_block_content_and_no_content(self):
        self.store.seek(8*512 - 256)
        self.store.write('X' * 512)
        self.assertEqual(MockConnection.block(7), '\xff'*256 + 'X'*256)
        self.assertEqual(MockConnection.block(8), 'X'*256 + '\0'*256)

    def test_seek_bad_offset(self):
        self.assertRaises(IOError, self.store.seek, -1)
        self.assertRaises(IOError, self.store.seek, 10000000000000)
        try:
            self.store.seek(-1)
        except IOError as ex:
            self.assertEqual(ex.errno, errno.ESPIPE)
        else:
            self.fail("didn't raise IOError")

    def test_tell(self):
        self.store.seek(0)
        self.assertEqual(self.store.tell(), 0)
        self.store.seek(1024)
        self.assertEqual(self.store.tell(), 1024)

    def test_read_end_of_disk(self):
        self.store.seek(15*512)
        data = self.store.read(1024)
        self.assertEqual(len(data), 512)

    def test_wite_end_of_disk(self):
        self.store.seek(15*512)
        self.assertRaises(IOError, self.store.write, 'X' * 1024)

class CacheTestCase(unittest.TestCase):
    """Test the block-split file class."""
    def setUp(self):
        from swiftnbd.common import Cache
        self.cache = Cache(10)

    def tearDown(self):
        pass

    def test_get_miss(self):
        data = self.cache.get(1)
        self.assertEqual(self.cache.ref[1], 0)
        self.assertEqual(data, None)

    def test_get_hit(self):
        self.cache.set(1, "DATA1")
        self.cache.set(2, "DATA2")

        data = self.cache.get(1)
        self.assertEqual(data, "DATA1")
        # set + get = 2 refferences
        self.assertEqual(self.cache.ref[1], 2)

        data = self.cache.get(2)
        self.assertEqual(data, "DATA2")

    def test_set(self):
        self.cache.set(1, "1")
        self.assertEqual(self.cache.ref[1], 1)
        self.cache.set(1, "1")
        self.assertEqual(self.cache.ref[1], 2)

    def test_limit(self):
        for i in range(10):
            self.cache.set(i, "DATA%s" % i)

        self.assertEqual(len(self.cache.data), 10)

        for i in range(10):
            self.assertEqual(self.cache.ref[i], 1)

        for i in range(10):
            for j in range(i+1):
                self.cache.get(i)

        # 0 should be discarded
        self.cache.set(10, "DATA11")

        self.assertEqual(len(self.cache.data), 10)
        self.assertEqual(self.cache.ref[0], 0)
        self.assertTrue(0 not in self.cache.data)

        # 10 should be discarded
        self.cache.set(11, "DATA12")

        self.assertEqual(len(self.cache.data), 10)
        self.assertEqual(self.cache.ref[10], 0)
        self.assertTrue(10 not in self.cache.data)

