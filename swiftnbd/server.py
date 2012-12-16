#!/usr/bin/env python
"""
swiftnbd. server module
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

import struct
import logging

from gevent.server import StreamServer

class Server(StreamServer):
    """
    Class implementing a StreamServer.
    """

    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3

    def __init__(self, listener, store, *kargs, **kwargs):
        super(Server, self).__init__(listener, *kargs, **kwargs)

        self.store = store
        self.log = logging.getLogger(__package__)

    def nbd_response(self, fo, handle, error=0, data=None):
        fo.write("\x67\x44\x66\x98" + struct.pack('>L', error) + struct.pack(">Q", handle))
        if data:
            fo.write(data)
        fo.flush()

    def handle(self, socket, address):
        host, port = address
        self.log.info("Incoming connection from %s:%s" % address)

        fo = socket.makefile()

        # initial handshake (old-style)
        fo.write("NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53" + struct.pack('>Q', self.store.size) + "\0"*128)
        fo.flush()

        while True:
            header = fo.read(28)
            (magic, cmd, handle, offset, length) = struct.unpack(">LLQQL", header)

            if magic != 0x25609513:
                self.log.error("[%s:%s]: Wrong magic number, disconnecting" % address)
                socket.close()
                return

            self.log.debug("[%s:%s]: cmd=%s, handle=%s, offset=%s, len=%s" % (host, port, cmd, handle, offset, length))

            if cmd == self.NBD_CMD_DISC:

                self.log.info("[%s:%s]: disconnecting" % address)
                socket.close()
                return

            elif cmd == self.NBD_CMD_WRITE:

                data = fo.read(length)
                if(len(data) != length):
                    self.log.error("[%s:%s]: %s bytes expected, disconnecting" % (host, port, length))
                    socket.close()
                    return

                try:
                    self.store.seek(offset)
                    self.store.write(data)
                except IOError as ex:
                    self.log.error("[%s:%s]: %s" % (host, port, ex))
                    self.nbd_response(fo, handle, error=ex.errno)
                    return

                self.nbd_response(fo, handle)

            elif cmd == self.NBD_CMD_READ:

                try:
                    self.store.seek(offset)
                    data = self.store.read(length)
                except IOError as ex:
                    self.log.error("[%s:%s]: %s" % (host, port, ex))
                    self.nbd_response(fo, handle, error=ex.errno)
                    return

                self.nbd_response(fo, handle, data=data)

            elif cmd == self.NBD_CMD_FLUSH:

                self.store.flush()

                self.nbd_response(fo, handle)

            else:

                self.log.warning("[%s:%s]: Unknown cmd %s, disconnecting" % (host, port, cmd))
                socket.close()
                return

