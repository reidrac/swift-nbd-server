#!/usr/bin/env python
"""
swiftnbd. common toolset
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

import logging
from logging.handlers import SysLogHandler
import os
from ConfigParser import RawConfigParser

class Stats(object):
    """Store and log stats."""
    def __init__(self, store):
        self.store = store

        self.bytes_in = 0
        self.bytes_out = 0
        self.log = logging.getLogger(__package__)

    def log_stats(self):
        """Log stats."""
        self.log.info("STATS: %s in=%s (%s), out=%s (%s)" % (self.store,
                                                             self.bytes_in,
                                                             self.store.bytes_out,
                                                             self.bytes_out,
                                                             self.store.bytes_in,
                                                             ))

        cache = len(self.store.cache) * self.store.object_size
        limit = self.store.cache.limit * self.store.object_size
        self.log.info("CACHE: %s size=%s, limit=%s (%.2f%%)" % (self.store, cache, limit, (cache*100.0/limit)))

class Credentials(object):
    """Credentials for accessing a container."""

    default_authurl = None

    def __init__(self, username, password, authurl, read_only):
        self.username = username
        self.password = password
        self.authurl = authurl or Credentials.default_authurl
        self.read_only = read_only

    def as_tuple(self):
        return (self.username, self.password, self.authurl)

_CONF_DEFAULTS = { 'username': None, 'password': None, 'authurl': None, 'read-only': '0', }

def getAllSecrets(secrets_file):
    """Read secrets for all containers"""
    conf = RawConfigParser(_CONF_DEFAULTS)
    conf.read(secrets_file)

    containers = dict()
    for container in conf.sections():
        containers[container] = Credentials(conf.get(container, 'username'),
                                            conf.get(container, 'password'),
                                            conf.get(container, 'authurl'),
                                            conf.getboolean(container, 'read-only')
                                            )
    return containers

def getSecrets(container, secrets_file):
    """Read secrets"""
    stat = os.stat(secrets_file)
    if stat.st_mode & 0x004 != 0:
        log = logging.getLogger(__package__)
        log.warning("%s is world readable, please consider changing its permissions to 0600" % secrets_file)

    conf = RawConfigParser(_CONF_DEFAULTS)
    conf.read(secrets_file)

    if not conf.has_section(container):
        raise ValueError("%s not found in %s" % (container, secrets_file))

    return Credentials(conf.get(container, 'username'),
                       conf.get(container, 'password'),
                       conf.get(container, 'authurl'),
                       conf.getboolean(container, 'read-only')
                       )

def getContainers(secrets_file):
    """Return a list of containers read from a secrets file"""
    conf = RawConfigParser(_CONF_DEFAULTS)
    conf.read(secrets_file)
    return conf.sections()

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

