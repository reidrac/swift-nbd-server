#!/usr/bin/env python

import unittest
from hashlib import md5

class MockConnection(object):
    """
    Mock up for the Swift client.

    Initialised with 2048 blocks of 512 bytes.
    """

    block_size = 512
    blocks = dict()

    class ClientException(Exception):
        def __init__(self, http_status=404):
            self.http_status = http_status

    def __init__(self):
        MockConnection.blocks = dict(("disk.part/%.8i" % block_num, '\xff'*MockConnection.block_size) for block_num in range(2048))

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
            return None, self.blocks[block_name]
        except KeyError:
            raise MockConnection.ClientException()

    def put_object(self, container, block_name, obj):
        data = obj.read()

        assert len(data) == self.block_size, "Data size mismatch"

        self.blocks[block_name] = data
        return md5(data).hexdigest()

class SwiftBlockFileTestCase(unittest.TestCase):
    """Test the block-split file class."""
    def setUp(self):
        # monkey patch the swiftclient to use out Mock up
        import swiftnbds.common as common
        common.client = MockConnection
        from swiftnbds.common import SwiftBlockFile

        # create a disk doubling the actual size of the mock up so we
        # have some uninitialised space to run tests
        self.store = SwiftBlockFile('url', 'user', 'pass', 'container', 512, 4096)

    def tearDown(self):
        pass

    def test_read_full_block_content(self):
        self.store.seek(0)
        data = self.store.read(512)
        self.assertEqual(data, '\xff'*512)

    def test_read_full_block_no_content(self):
        self.store.seek(2048*512)
        data = self.store.read(512)
        self.assertEqual(data, '\0'*512)

    def test_write_full_block(self):
        self.store.seek(0)
        self.store.write('X' * 512)
        self.assertEqual(MockConnection.block(0), 'X'*512)

        self.store.seek(2048*512)
        self.store.write('X' * 512)
        self.assertEqual(MockConnection.block(2048), 'X'*512)
