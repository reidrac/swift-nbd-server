#!/usr/bin/env python
"""
swiftnbd. object-split file manager
Copyright (C) 2013 by Juan J. Martinez <jjm@usebox.net>

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

import errno
from time import time
from cStringIO import StringIO
from hashlib import md5

from swiftclient import client

from swiftnbd.common import getMeta, setMeta
from swiftnbd.cache import Cache

class SwiftStorage(object):
    """
    Manages a object-split file stored in OpenStack Object Storage (swift).

    May raise IOError.
    """
    cache = None

    def __init__(self, authurl, username, password, container, object_size, objects, cache=None):
        self.container = container
        self.object_size = object_size
        self.objects = objects
        self.pos = 0
        self.locked = False
        self.meta = dict()

        self.bytes_in = 0
        self.bytes_out = 0

        self.cache = cache
        if self.cache is None:
            self.cache = Cache(int(1024**2 / self.object_size))

        self.cli = client.Connection(authurl, username, password)

    def __str__(self):
        return self.container

    def lock(self, client_id):
        """Set the storage as busy"""
        if self.locked:
            return

        try:
            headers, _ = self.cli.get_container(self.container)
        except client.ClientException as ex:
            raise IOError(errno.EACCES, "Storage error: %s" % ex.http_status)

        self.meta = getMeta(headers)

        if self.meta.get('client'):
            raise IOError(errno.EBUSY, "Storage already in use: %s" % self.meta['client'])

        self.meta['client'] = "%s@%i" % (client_id, time())
        hdrs = setMeta(self.meta)
        try:
            self.cli.put_container(self.container, headers=hdrs)
        except client.ClientException as ex:
            raise IOError(errno.EIO, "Failed to lock: %s" % ex.http_status)

        self.locked = True

    def unlock(self):
        """Set the storage as free"""
        if not self.locked:
            return

        self.meta['last'] = self.meta.get('client')
        self.meta['client'] = ''
        hdrs = setMeta(self.meta)
        try:
            self.cli.put_container(self.container, headers=hdrs)
        except client.ClientException as ex:
            raise IOError(errno.EIO, "Failed to unlock: %s" % ex.http_status)

        self.locked = False

    def read(self, size):
        data = ""
        _size = size
        while _size > 0:
            obj = self.fetch_object(self.object_num)
            if obj == '':
                break

            if _size + self.object_pos >= self.object_size:
                data += obj[self.object_pos:]
                part_size = self.object_size - self.object_pos
            else:
                data += obj[self.object_pos:self.object_pos+_size]
                part_size = _size

            _size -= part_size
            self.seek(self.pos + part_size)

        return data

    def write(self, data):
        _data = data[:]
        if self.object_pos != 0:
            # object-align the beginning of data
            obj = self.fetch_object(self.object_num)
            _data = obj[:self.object_pos] + _data
            self.seek(self.pos - self.object_pos)

        reminder = len(_data) % self.object_size
        if reminder != 0:
            # object-align the end of data
            obj = self.fetch_object(self.object_num + (len(_data) / self.object_size))
            _data += obj[reminder:]

        assert len(_data) % self.object_size == 0, "Data not aligned!"

        offs = 0
        object_num = self.object_num
        while offs < len(_data):
            self.put_object(object_num, _data[offs:offs+self.object_size])
            offs += self.object_size
            object_num += 1

    def tell(self):
        return self.pos

    @property
    def object_pos(self):
        # position in the object
        return self.pos % self.object_size

    @property
    def object_num(self):
        # object number based on the position
        return self.pos / self.object_size

    @property
    def size(self):
        return self.object_size * self.objects

    def flush(self):
        self.cache.flush()

    def object_name(self, object_num):
        return "disk.part/%.8i" % object_num

    def fetch_object(self, object_num):
        if object_num >= self.objects:
            return ''

        data = self.cache.get(object_num)
        if not data:
            object_name = self.object_name(object_num)
            try:
                _, data = self.cli.get_object(self.container, object_name)
            except client.ClientException as ex:
                if ex.http_status != 404:
                    raise IOError(errno.EIO, "Storage error: %s" % ex)
                return '\0' * self.object_size

            self.bytes_in += self.object_size
            self.cache.set(object_num, data)
        return data

    def put_object(self, object_num, data):
        if object_num >= self.objects:
            raise IOError(errno.ESPIPE, "Write offset out of bounds")

        object_name = self.object_name(object_num)
        try:
            etag = self.cli.put_object(self.container, object_name, StringIO(data))
        except client.ClientException as ex:
            raise IOError("Storage error: %s" % ex, errno=errno.EIO)

        checksum = md5(data).hexdigest()
        etag = etag.lower()
        if etag != checksum:
            raise IOError(errno.EAGAIN, "Block integrity error (object_num=%s)" % object_num)

        self.bytes_out += self.object_size
        self.cache.set(object_num, data)

    def seek(self, offset):
        if offset < 0 or offset > self.size:
            raise IOError(errno.ESPIPE, "Offset out of bounds")

        self.pos = offset

