#!/usr/bin/env python
"""
swiftnbd. main module
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

from argparse import ArgumentParser
import struct

from swiftclient import client
from gevent.server import StreamServer

from swiftnbd.const import version, description, project_url, auth_url, secrets_file
from swiftnbd.common import setLog, getMeta, getSecrets, SwiftBlockFile

class Main(object):
    def __init__(self):

        self.meta = None
        self.block_size = 0
        self.blocks = 0
        self.export_size = 0

        parser = ArgumentParser(description=description,
                                epilog="Contact and support: %s" % project_url
                                )

        parser.add_argument("container", help="container used as storage (setup it first)")

        parser.add_argument("--version", action="version", version="%(prog)s "  + version)

        parser.add_argument("--secrets", dest="secrets_file",
                            default=secrets_file,
                            help="filename containing user/password (default: %s)" % secrets_file)

        parser.add_argument("-a", "--auth-url", dest="authurl",
                            default=auth_url,
                            help="authentication URL (default: %s)" % auth_url)

        parser.add_argument("-b", "--bind-address", dest="bind_address",
                            default="127.0.0.1",
                            help="bind address (default: 127.0.0.1)")

        parser.add_argument("-p", "--bind-port", dest="bind_port",
                            type=int,
                            default=10811,
                            help="bind address (default: 10811)")

        parser.add_argument("-v", "--verbose", dest="verbose",
                            action="store_true",
                            help="enable verbose logging"
                            )

        self.args = parser.parse_args()

        self.log = setLog(debug=self.args.verbose)
        self.log.debug(dict((k, v) for k, v in vars(self.args).iteritems() if k != "password"))

        try:
            self.username, self.password = getSecrets(self.args.container, self.args.secrets_file)
        except ValueError as ex:
            parser.error(ex)

    def run(self):

        cli = client.Connection(self.args.authurl, self.username, self.password)

        try:
            headers, _ = cli.get_container(self.args.container)
        except client.ClientException as ex:
            if ex.http_status == 404:
                self.log.error("%s doesn't exist" % self.args.container)
                return 1
            else:
                self.log.error(ex)
                return 1
        else:
            self.log.debug(headers)

            self.meta = getMeta(headers)
            if not self.meta:
                self.log.error("%s doesn't appear to be setup" % self.args.container)
                return 1

            self.log.debug("Meta: %s" % self.meta)

            try:
                self.block_size = int(self.meta['block-size'])
                self.blocks = int(self.meta['blocks'])
            except ValueError as ex:
                self.log.error("%s doesn't appear to be correct: %s" % (self.args.container, ex))
                return 1

            self.export_size = self.block_size * self.blocks

        addr = (self.args.bind_address, self.args.bind_port)
        server = StreamServer(addr, self.handle_request)

        self.log.info("Starting server on %s:%s" % addr)
        try:
            server.serve_forever()
        except KeyboardInterrupt as ex:
            self.log.warning("Interrupt received")

        self.log.info("Exiting...")
        return 0

    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3

    def nbd_response(self, fo, handle, error=0, data=None):
        fo.write("\x67\x44\x66\x98" + struct.pack('>L', error) + struct.pack(">Q", handle))
        if data:
            fo.write(data)
        fo.flush()

    def handle_request(self, socket, address):
        host, port = address
        self.log.info("Incoming connection from %s:%s" % address)

        store = SwiftBlockFile(self.args.authurl, self.username, self.password, self.args.container, self.block_size, self.blocks)
        fo = socket.makefile()

        # initial handshake (old-style)
        fo.write("NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53" + struct.pack('>Q', self.export_size) + "\0"*128)
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
                    store.seek(offset)
                    store.write(data)
                except IOError as ex:
                    self.log.error("[%s:%s]: %s" % (host, port, ex))
                    self.nbd_response(fo, handle, error=ex.errno)
                    return

                self.nbd_response(fo, handle)

            elif cmd == self.NBD_CMD_READ:

                try:
                    store.seek(offset)
                    data = store.read(length)
                except IOError as ex:
                    self.log.error("[%s:%s]: %s" % (host, port, ex))
                    self.nbd_response(fo, handle, error=ex.errno)
                    return

                self.nbd_response(fo, handle, data=data)

            elif cmd == self.NBD_CMD_FLUSH:

                store.flush()

                self.nbd_response(fo, handle)

            else:

                self.log.warning("[%s:%s]: Unknown cmd %s, disconnecting" % (host, port, cmd))
                socket.close()
                return

