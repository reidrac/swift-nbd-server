#!/usr/bin/env python
"""
swiftnbd. cache management
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
from collections import Counter

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

    def __len__(self):
        return len(self.data)

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
            for key, _ in less_used:
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

