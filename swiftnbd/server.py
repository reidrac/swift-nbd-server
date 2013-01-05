#!/usr/bin/env python
"""
swiftnbd. server module
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

import struct
import logging

from gevent.server import StreamServer
from gevent import spawn_later

from swiftnbd.const import stats_delay

class Server(StreamServer):
    """
    Class implementing a StreamServer.
    """

    # NBD's magic
    NBD_HANDSHAKE = "NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53"
    NBD_RESPONSE = "\x67\x44\x66\x98"

    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3

    def __init__(self, listener, store):
        super(Server, self).__init__(listener)

        self.bytes_in = 0
        self.bytes_out = 0

        self.store = store
        self.log = logging.getLogger(__package__)
        self.log_stats()

    def log_stats(self):
        """Log periodically server stats"""
        self.log.info("STATS: in=%s (%s), out=%s (%s)" % (self.bytes_in,
                                                          self.store.bytes_out,
                                                          self.bytes_out,
                                                          self.store.bytes_in,
                                                          ))

        cache = len(self.store.cache) * self.store.object_size
        limit = self.store.cache.limit * self.store.object_size
        self.log.info("CACHE: size=%s, limit=%s (%.2f%%)" % (cache, limit, (cache*100.0/limit)))

        spawn_later(stats_delay, self.log_stats)

    def nbd_response(self, fob, handle, error=0, data=None):
        fob.write(self.NBD_RESPONSE + struct.pack('>L', error) + struct.pack(">Q", handle))
        if data:
            fob.write(data)
        fob.flush()

    def handle(self, socket, address):
        host, port = address
        self.log.info("Incoming connection from %s:%s" % address)

        try:
            self.store.lock("%s:%s" % address)
        except IOError as ex:
            self.log.error("[%s:%s]: %s" % (host, port, ex))
            socket.close()
            return

        fob = socket.makefile()

        # initial handshake (old-style)
        fob.write(self.NBD_HANDSHAKE + struct.pack('>Q', self.store.size) + "\x00"*128)
        fob.flush()

        while True:
            header = fob.read(28)
            try:
                (magic, cmd, handle, offset, length) = struct.unpack(">LLQQL", header)
            except struct.error:
                self.log.error("[%s:%s]: Invalid request, disconnecting" % address)
                break

            if magic != 0x25609513:
                self.log.error("[%s:%s]: Wrong magic number, disconnecting" % address)
                socket.close()
                break

            self.log.debug("[%s:%s]: cmd=%s, handle=%s, offset=%s, len=%s" % (host, port, cmd, handle, offset, length))

            if cmd == self.NBD_CMD_DISC:

                self.log.info("[%s:%s]: disconnecting" % address)
                break

            elif cmd == self.NBD_CMD_WRITE:

                data = fob.read(length)
                if(len(data) != length):
                    self.log.error("[%s:%s]: %s bytes expected, disconnecting" % (host, port, length))
                    break

                try:
                    self.store.seek(offset)
                    self.store.write(data)
                except IOError as ex:
                    self.log.error("[%s:%s]: %s" % (host, port, ex))
                    self.nbd_response(fob, handle, error=ex.errno)
                    continue

                self.bytes_in += length
                self.nbd_response(fob, handle)

            elif cmd == self.NBD_CMD_READ:

                try:
                    self.store.seek(offset)
                    data = self.store.read(length)
                except IOError as ex:
                    self.log.error("[%s:%s]: %s" % (host, port, ex))
                    self.nbd_response(fob, handle, error=ex.errno)
                    continue

                if data:
                    self.bytes_out += len(data)
                self.nbd_response(fob, handle, data=data)

            elif cmd == self.NBD_CMD_FLUSH:

                self.store.flush()
                self.nbd_response(fob, handle)

            else:

                self.log.warning("[%s:%s]: Unknown cmd %s, disconnecting" % (host, port, cmd))
                break

        try:
            self.store.unlock()
        except IOError as ex:
            self.log.error(ex)
        socket.close()

