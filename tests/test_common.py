#!/usr/bin/env python

import unittest
from hashlib import md5

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
        import swiftnbds.common as common
        common.client = MockConnection
        from swiftnbds.common import SwiftBlockFile

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

    def test_tell(self):
        self.store.seek(0)
        self.assertEqual(self.store.tell(), 0)
        self.store.seek(1024)
        self.assertEqual(self.store.tell(), 1024)

