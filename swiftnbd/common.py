#!/usr/bin/env python
"""
swiftnbd. common toolset
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

import logging
import errno
import os
from cStringIO import StringIO
from hashlib import md5
from ConfigParser import RawConfigParser

from swiftclient import client

def getSecrets(container, secrets_file):
    """Read secrets"""

    stat = os.stat(secrets_file)
    if stat.st_mode & 0x004 != 0:
        log = logging.getLogger(__package__)
        log.warn("%s is world readable, please consider changing its permissions to 0600" % secrets_file)

    conf = RawConfigParser(dict(username=None, password=None))
    conf.read(secrets_file)

    if not conf.has_section(container):
        raise ValueError("%s not found in %s" % (container, secrets_file))

    return (conf.get(container, 'username'), conf.get(container, 'password'))

def setLog(debug=False):
    """Setup logger"""

    log = logging.getLogger(__package__)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s: %(name)s: %(levelname)s: %(message)s'))
    log.addHandler(handler)

    if debug:
        log.setLevel(logging.DEBUG)
        log.debug("Verbose log enabled")
    else:
        log.setLevel(logging.INFO)

    return log

_META_PREFIX = "x-container-meta-swiftnbd-"
_META_REQUIRED = ('version', 'blocks', 'block-size')

def setMeta(meta):
    """Convert a metada dict into swift meta headers"""
    return dict(("%s%s" % (_META_PREFIX, key), value) for key, value in meta.iteritems())

def getMeta(hdrs):
    """Convert swift meta headers with swiftndb prefix into a dictionary"""
    data = dict((key[len(_META_PREFIX):], value) for key, value in hdrs.iteritems() if key.lower().startswith(_META_PREFIX))
    for key in _META_REQUIRED:
        if key not in data:
            return dict()
    return data

class SwiftBlockFile(object):
    """
    Manages a block-split file stored in OpenStack Object Storage (swift).

    May raise IOError.
    """
    cache = dict()

    def __init__(self, authurl, username, password, container, block_size, blocks):
        self.container = container
        self.block_size = block_size
        self.size = block_size * blocks
        self.pos = 0

        self.cli = client.Connection(authurl, username, password)

        # force authentication
        try:
            self.cli.get_container(container)
        except client.ClientException as ex:
            raise IOError(errno.EACCES, "Storage error: %s" % ex.http_status)

    def read(self, size):

        data = ""
        while size > 0:
            block = self.fetch_block(self.block_num)

            if size + self.block_pos >= self.block_size:
                data += block[self.block_pos:]
                part_size = self.block_size - self.block_pos
            else:
                data += block[self.block_pos:self.block_pos+size]
                part_size = size

            size -= part_size
            self.seek(self.pos + part_size)

        return data

    def write(self, data):

        if self.block_pos != 0:
            # block-align the beginning of data
            block = self.fetch_block(self.block_num)
            data = block[:self.block_pos] + data
            self.seek(self.pos - self.block_pos)

        reminder = len(data) % self.block_size
        if reminder != 0:
            # block-align the end of data
            block = self.fetch_block(self.block_num + (len(data) / self.block_size))
            data += block[reminder:]

        assert len(data) % self.block_size == 0, "Data not aligned!"

        offs = 0
        block_num = self.block_num
        while offs < len(data):
            self.put_block(block_num, data[offs:offs+self.block_size])
            offs += self.block_size
            block_num += 1

    def tell(self):
        return self.pos

    @property
    def block_pos(self):
        # position in the block
        return self.pos % self.block_size

    @property
    def block_num(self):
        # block number based on the position
        return self.pos / self.block_size

    def flush(self):
        self.cache = dict()

    def fetch_block(self, block_num):

        data = self.cache.get(block_num)
        if not data:
            block_name = "disk.part/%.8i" % block_num
            try:
                _, data = self.cli.get_object(self.container, block_name)
            except client.ClientException as ex:
                if ex.http_status != 404:
                    raise IOError(errno.EIO, "Storage error: %s" % ex)
                return '\0' * self.block_size

            self.cache[block_num] = data
        return data

    def put_block(self, block_num, data):

        block_name = "disk.part/%.8i" % block_num
        try:
            etag = self.cli.put_object(self.container, block_name, StringIO(data))
        except client.ClientException as ex:
            raise IOError("Storage error: %s" % ex, errno=errno.EIO)

        checksum = md5(data).hexdigest()
        etag = etag.lower()
        if etag != checksum:
            raise IOError(errno.EAGAIN, "Block integrity error (block_num=%s)" % block_num)

        self.cache[block_num] = data

    def seek(self, offset):
        if offset < 0 or offset > self.size:
            raise IOError(errno.ESPIPE, "Offset out of bounds")

        self.pos = offset

