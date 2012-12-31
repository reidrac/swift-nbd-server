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
from logging.handlers import SysLogHandler
import errno
import os
from time import time
from cStringIO import StringIO
from collections import Counter
from hashlib import md5
from ConfigParser import RawConfigParser

from swiftclient import client

def getSecrets(container, secrets_file):
    """Read secrets"""
    stat = os.stat(secrets_file)
    if stat.st_mode & 0x004 != 0:
        log = logging.getLogger(__package__)
        log.warning("%s is world readable, please consider changing its permissions to 0600" % secrets_file)

    conf = RawConfigParser(dict(username=None, password=None))
    conf.read(secrets_file)

    if not conf.has_section(container):
        raise ValueError("%s not found in %s" % (container, secrets_file))

    return (conf.get(container, 'username'), conf.get(container, 'password'))

def setLog(debug=False, use_syslog=False, use_file=None):
    """Setup logger"""
    log = logging.getLogger(__package__)

    if use_syslog:
        try:
            handler = SysLogHandler(address="/dev/log", facility='local0')
        except IOError:
            # fallback to UDP
            handler = SysLogHandler(facility='local0')
        handler.setFormatter(logging.Formatter('%(name)s[%(process)d]: %(levelname)s: %(message)s'))
    else:
        if use_file:
            handler = logging.FileHandler(use_file)
        else:
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
_META_REQUIRED = ('version', 'objects', 'object-size')

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

class Cache(object):
    """
    Cache manager.

    This is an in-memory cache manager that stores up to 'limit' items (objects),
    releasing the least frequently used when the limit is reached.
    """
    def __init__(self, limit):

        self.limit = limit
        self.ref = Counter()
        self.data = dict()

        self.log = logging.getLogger(__package__)
        self.log.debug("cache size: %s" % self.limit)

    def get(self, object_name, default=None):
        """Get an element from the cache"""
        if self.ref[object_name] > 0:
            self.ref[object_name] += 1

            self.log.debug("cache get hit: %s, %s" % (object_name, self.ref[object_name]))
            return self.data[object_name]

        self.log.debug("cache get miss: %s, %s" % (object_name, self.ref[object_name]))
        return default

    def set(self, object_name, data):
        """Put/update an element in the cache"""
        self.data[object_name] = data
        self.ref[object_name] += 1

        self.log.debug("cache set: %s, %s" % (object_name, self.ref[object_name]))

        if len(self.data) > self.limit:
            self.log.debug("cache size is over limit (%s > %s)" % (len(self.data), self.limit))
            less_used = self.ref.most_common()[:-3:-1]
            for key, freq in less_used:
                if object_name != key:
                    self.log.debug("cache free: %s, %s" % (key, self.ref[key]))
                    del self.ref[key]
                    del self.data[key]
                    break

    def flush(self):
        """Flush the cache"""
        self.log.debug("cache flush, was (%s): %s" % (len(self.data), self.ref))
        self.ref = Counter()
        self.data = dict()

class SwiftBlockFile(object):
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

        self.cache = cache or Cache(int(1024**2 / self.object_size))

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

        self.cache.set(object_num, data)

    def seek(self, offset):
        if offset < 0 or offset > self.size:
            raise IOError(errno.ESPIPE, "Offset out of bounds")

        self.pos = offset

