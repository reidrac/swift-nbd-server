#!/usr/bin/env python

import logging
from cStringIO import StringIO

from swiftclient import client

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
            raise IOError("Storage error: %s" % ex.http_status)

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
            block = self.fetch_block(len(data) / self.block_size)
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
                    raise IOError("Storage error: %s" % ex)
                return '\0' * self.block_size

            self.cache[block_num] = data
        return data

    def put_block(self, block_num, data):

        block_name = "disk.part/%.8i" % block_num
        try:
            self.cli.put_object(self.container, block_name, StringIO(data))
        except client.ClientException as ex:
            raise IOError("Storage error: %s" % ex)

        self.cache[block_num] = data

    def seek(self, offset):
        if offset < 0 or offset > self.size:
            raise IOError("Offset out of bounds")

        self.pos = offset

