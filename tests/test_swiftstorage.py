#!/usr/bin/env python
"""
swiftnbd. tests for the common module
Copyright (C) 2013-2015 by Juan J. Martinez <jjm@usebox.net>

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
from io import StringIO
import errno

class MockConnection(object):
    """
    Mock up for the Swift client.

    Initialised with 8 objects of 512 bytes.
    """

    object_size = 512
    objects = dict()

    class ClientException(Exception):
        def __init__(self, http_status=404):
            self.http_status = http_status

    def __init__(self):
        MockConnection.objects = dict(("disk.part/%08i" % object_num, b'\xff'*MockConnection.object_size) for object_num in range(8))

    @staticmethod
    def object(object_num):
        return MockConnection.objects["disk.part/%08i" % object_num]

    @staticmethod
    def Connection(authurl, username, password):
        return MockConnection()

    def get_container(self, container):
        pass

    def get_object(self, container, object_name):
        try:
            return None, MockConnection.objects[object_name]
        except KeyError:
            raise MockConnection.ClientException()

    def put_object(self, container, object_name, data):
        assert len(data) == self.object_size, "Data size mismatch"
        MockConnection.objects[object_name] = data
        return md5(data).hexdigest()

class SwiftStorageTestCase(unittest.TestCase):
    """Test the object-split file class."""
    def setUp(self):
        # monkey-patch swiftclient to use out mock up
        import swiftnbd.swift as swift
        swift.client = MockConnection
        from swiftnbd.swift import SwiftStorage

        # create a disk doubling the actual size of the mock up so we
        # have some uninitialised space to run tests
        self.store = SwiftStorage('url', 'user', 'pass', 'container', 512, 16)
        self.store.flush()

    def tearDown(self):
        pass

    def test_read_full_object_content(self):
        self.store.seek(0)
        data = self.store.read(512)
        self.assertEqual(data, b'\xff'*512)

    def test_read_full_object_no_content(self):
        self.store.seek(8*512)
        data = self.store.read(512)
        self.assertEqual(data, b'\0'*512)

    def test_write_full_object(self):
        self.store.seek(0)
        self.store.write(b'X'*512)
        self.assertEqual(MockConnection.object(0), b'X'*512)

        self.store.seek(8*512)
        self.store.write(b'X'*512)
        self.assertEqual(MockConnection.object(8), b'X'*512)

    def test_read_partial_object_content(self):
        self.store.seek(0)
        data = self.store.read(256)
        self.assertEqual(data, b'\xff'*256)

    def test_read_partial_object_no_content(self):
        self.store.seek(8*512)
        data = self.store.read(256)
        self.assertEqual(data, b'\0'*256)

    def test_write_partial_object_content(self):
        self.store.seek(0)
        self.store.write(b'X'*256)
        self.assertEqual(MockConnection.object(0), b'X'*256 + b'\xff'*256)

    def test_write_partial_object_no_content(self):
        self.store.seek(8*512)
        self.store.write(b'X'*256)
        self.assertEqual(MockConnection.object(8), b'X'*256 + b'\0'*256)

    def test_read_inter_object_content(self):
        self.store.seek(256)
        data = self.store.read(512)
        self.assertEqual(data, b'\xff'*512)

    def test_read_inter_object_no_content(self):
        self.store.seek(8*512 + 256)
        data = self.store.read(512)
        self.assertEqual(data, b'\0'*512)

    def test_read_inter_object_content_and_no_content(self):
        self.store.seek(8*512 - 256)
        data = self.store.read(512)
        self.assertEqual(data, b'\xff'*256 +  b'\0'*256)

    def test_write_inter_object_content(self):
        self.store.seek(256)
        self.store.write(b'X'*512)
        self.assertEqual(MockConnection.object(0), b'\xff'*256 + b'X'*256)
        self.assertEqual(MockConnection.object(1), b'X'*256 + b'\xff'*256)

    def test_write_inter_object_no_content(self):
        self.store.seek(8*512 + 256)
        self.store.write(b'X'*512)
        self.assertEqual(MockConnection.object(8), b'\0'*256 + b'X'*256)
        self.assertEqual(MockConnection.object(9), b'X'*256 + b'\0'*256)

    def test_write_inter_object_content_and_no_content(self):
        self.store.seek(8*512 - 256)
        self.store.write(b'X'*512)
        self.assertEqual(MockConnection.object(7), b'\xff'*256 + b'X'*256)
        self.assertEqual(MockConnection.object(8), b'X'*256 + b'\0'*256)

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
        self.assertRaises(IOError, self.store.write, b'X'*1024)

