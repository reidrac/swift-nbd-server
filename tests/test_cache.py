#!/usr/bin/env python
"""
swiftnbd. tests for the cache module
Copyright (C) 2013-2016 by Juan J. Martinez <jjm@usebox.net>

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

class CacheTestCase(unittest.TestCase):
    """Test the cache class."""
    def setUp(self):
        from swiftnbd.cache import Cache
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

