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
from swiftnbd.common import Stats

class Server(StreamServer):
    """
    Class implementing a StreamServer.
    """

    # NBD's magic
    NBD_HANDSHAKE = 0x49484156454F5054
    NBD_REPLY = 0x3e889045565a9

    NBD_REQUEST = 0x25609513
    NBD_RESPONSE = 0x67446698

    NBD_OPT_EXPORTNAME = 1
    NBD_OPT_ABORT = 2
    NBD_OPT_LIST = 3

    NBD_REP_ACK = 1
    NBD_REP_SERVER = 2
    NBD_REP_ERR_UNSUP = 2**31 + 1

    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3

    # fixed newstyle handshake
    NBD_HANDSHAKE_FLAGS = (1 << 0)

    # has flags, supports flush
    NBD_EXPORT_FLAGS = (1 << 0) ^ (1 << 2)
    NBD_RO_FLAG = (1 << 1)

    def __init__(self, listener, stores):
        super(Server, self).__init__(listener)

        self.log = logging.getLogger(__package__)

        self.stores = stores

        self.stats = dict()
        for store in self.stores.values():
            self.stats[store] = Stats(store)
        self.log_stats()

        self.set_handle(self.handler)

    def log_stats(self):
        """Log periodically server stats"""
        for stats in self.stats.values():
            stats.log_stats()

        spawn_later(stats_delay, self.log_stats)

    def nbd_response(self, fob, handle, error=0, data=None):
        fob.write(struct.pack('>LLQ', self.NBD_RESPONSE, error, handle))
        if data:
            fob.write(data)
        fob.flush()

    def handler(self, socket, address):
        host, port = address
        store, container = None, None
        self.log.info("Incoming connection from %s:%s" % address)

        try:
            fob = socket.makefile()

            # initial handshake
            fob.write("NBDMAGIC" + struct.pack(">QH", self.NBD_HANDSHAKE, self.NBD_HANDSHAKE_FLAGS))
            fob.flush()

            data = fob.read(4)
            try:
                client_flag = struct.unpack(">L", data)[0]
            except struct.error:
                raise IOError("Handshake failed, disconnecting")

            # we support both fixed and unfixed new-style handshake
            if client_flag == 0:
                fixed = False
                self.log.warning("Client using new-style non-fixed handshake")
            elif client_flag & 1 == 1:
                fixed = True
            else:
                raise IOError("Handshake failed, disconnecting")

            # negotiation phase
            while True:
                header = fob.read(16)
                try:
                    (magic, opt, length) = struct.unpack(">QLL", header)
                except struct.error:
                    raise IOError("Negotiation failed: Invalid request, disconnecting")

                if magic != self.NBD_HANDSHAKE:
                    raise IOError("Negotiation failed: bad magic number: %s" % magic)

                if length:
                    data = fob.read(length)
                    if(len(data) != length):
                        raise IOError("Negotiation failed: %s bytes expected" % length)
                else:
                    data = None

                self.log.debug("[%s:%s]: opt=%s, len=%s, data=%s" % (host, port, opt, length, data))

                if opt == self.NBD_OPT_EXPORTNAME:

                    if not data:
                        raise IOError("Negotiation failed: no export name was provided")

                    if data not in self.stores:
                        if not fixed:
                            raise IOError("Negotiation failed: unknown export name")

                        fob.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ERR_UNSUP, 0))
                        fob.flush()
                        continue

                    # we have negotiated a store and it will be used
                    # until the client disconnects
                    store = self.stores[data]

                    store.lock("%s:%s" % address)

                    self.log.info("[%s:%s] Negotiated export: %s" % (host, port, store.container))

                    export_flags = self.NBD_EXPORT_FLAGS
                    if store.read_only:
                        export_flags ^= self.NBD_RO_FLAG
                        self.log.info("[%s:%s] %s is read only" % (host, port, store.container))
                    fob.write(struct.pack('>QH', store.size, export_flags) + "\x00"*124)
                    fob.flush()
                    break

                elif opt == self.NBD_OPT_LIST:

                    for container in self.stores.keys():
                        fob.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_SERVER, len(container) + 4))
                        fob.write(struct.pack(">L", len(container)) + container)
                        fob.flush()

                    fob.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ACK, 0))
                    fob.flush()

                elif opt == self.NBD_OPT_ABORT:

                    fob.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ACK, 0))
                    fob.flush()

                    raise IOError("Client aborted negotiation")

                else:
                    # we don't support any other option
                    if not fixed:
                        raise IOError("Unsupported option")

                    fob.write(struct.pack(">QLLL", self.NBD_REPLY, opt, self.NBD_REP_ERR_UNSUP, 0))
                    fob.flush()

            # operation phase
            while True:
                header = fob.read(28)
                try:
                    (magic, cmd, handle, offset, length) = struct.unpack(">LLQQL", header)
                except struct.error:
                    raise IOError("Invalid request, disconnecting")

                if magic != self.NBD_REQUEST:
                    raise IOError("Bad magic number, disconnecting")

                self.log.debug("[%s:%s]: cmd=%s, handle=%s, offset=%s, len=%s" % (host, port, cmd, handle, offset, length))

                if cmd == self.NBD_CMD_DISC:

                    self.log.info("[%s:%s] disconnecting" % address)
                    break

                elif cmd == self.NBD_CMD_WRITE:

                    data = fob.read(length)
                    if(len(data) != length):
                        raise IOError("%s bytes expected, disconnecting" % length)

                    try:
                        store.seek(offset)
                        store.write(data)
                    except IOError as ex:
                        self.log.error("[%s:%s] %s" % (host, port, ex))
                        self.nbd_response(fob, handle, error=ex.errno)
                        continue

                    self.stats[store].bytes_in += length
                    self.nbd_response(fob, handle)

                elif cmd == self.NBD_CMD_READ:

                    try:
                        store.seek(offset)
                        data = store.read(length)
                    except IOError as ex:
                        self.log.error("[%s:%s] %s" % (host, port, ex))
                        self.nbd_response(fob, handle, error=ex.errno)
                        continue

                    if data:
                        self.stats[store].bytes_out += len(data)
                    self.nbd_response(fob, handle, data=data)

                elif cmd == self.NBD_CMD_FLUSH:

                    store.flush()
                    self.nbd_response(fob, handle)

                else:

                    self.log.warning("[%s:%s] Unknown cmd %s, disconnecting" % (host, port, cmd))
                    break

        except IOError as ex:
            self.log.error("[%s:%s] %s" % (host, port, ex))

        finally:
            if store:
                try:
                    store.unlock()
                except IOError as ex:
                    self.log.error(ex)

            socket.close()

    def unlock_all(self):
        """Unlock any locked storage."""
        for store in self.stores.values():
            if store.locked:
                self.log.debug("%s: Unlocking storage..." % store)
                store.unlock()

