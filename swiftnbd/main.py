#!/usr/bin/env python
"""
swiftnbd. main module
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

import os
import sys
import signal
import socket
import tempfile
from argparse import ArgumentParser

import gevent
from swiftclient import client

from swiftnbd.const import version, description, project_url, auth_url, secrets_file, disk_version
from swiftnbd.common import setLog, getMeta, Config
from swiftnbd.cache import Cache
from swiftnbd.swift import SwiftStorage
from swiftnbd.server import Server

class Main(object):
    def __init__(self):

        self.meta = None
        self.object_size = 0
        self.objects = 0
        self.export_size = 0

        parser = ArgumentParser(description=description,
                                epilog="Contact and support: %s" % project_url
                                )

        parser.add_argument("--version", action="version", version="%(prog)s "  + version)

        parser.add_argument("--secrets", dest="secrets_file",
                            default=secrets_file,
                            help="filename containing user/password (default: %s)" % secrets_file)

        parser.add_argument("-a", "--auth-url", dest="authurl",
                            default=auth_url,
                            help="default authentication URL (default: %s)" % auth_url)

        parser.add_argument("-b", "--bind-address", dest="bind_address",
                            default="127.0.0.1",
                            help="bind address (default: 127.0.0.1)")

        parser.add_argument("-p", "--bind-port", dest="bind_port",
                            type=int,
                            default=10809,
                            help="bind address (default: 10809)")

        parser.add_argument("-c", "--cache-limit", dest="cache_limit",
                            type=int,
                            default=64,
                            help="cache memory limit in MB (default: 64)")

        parser.add_argument("-l", "--log-file", dest="log_file",
                            default=None,
                            help="log into the provided file"
                            )

        parser.add_argument("--syslog", dest="syslog",
                            action="store_true",
                            help="log to system logger (local0)"
                            )

        parser.add_argument("-f", "--foreground", dest="foreground",
                            action="store_true",
                            help="don't detach from terminal (foreground mode)"
                            )

        default_pidfile = os.path.join(tempfile.gettempdir(), "%s.pid" % __package__)
        parser.add_argument("--pid-file", dest="pidfile",
                            default=default_pidfile,
                            help="filename to store the PID (default: %s)" % default_pidfile
                            )

        parser.add_argument("-v", "--verbose", dest="verbose",
                            action="store_true",
                            help="enable verbose logging"
                            )

        self.args = parser.parse_args()

        if self.args.cache_limit < 1:
            parser.error("Cache limit can't be less than 1MB")

        self.log = setLog(debug=self.args.verbose, use_syslog=self.args.syslog, use_file=self.args.log_file)

        try:
            self.conf = Config(self.args.secrets_file, self.args.authurl)
        except OSError as ex:
            parser.error("Failed to load secrets: %s" % ex)

    def run(self):

        if os.path.isfile(self.args.pidfile):
            self.log.error("%s found: is the server already running?" % self.args.pidfile)
            return 1

        stores = dict()
        for container, values in self.conf.iteritems():
            cli = client.Connection(values['authurl'], values['username'], values['password'])

            try:
                headers, _ = cli.get_container(container)
            except (socket.error, client.ClientException) as ex:
                if getattr(ex, 'http_status', None) == 404:
                    self.log.warning("%s doesn't exist, skipping" % container)
                    continue
                else:
                    self.log.error(ex)
                    return 1

            self.log.debug(headers)

            meta = getMeta(headers)
            if not meta:
                self.log.warning("%s doesn't appear to be setup, skipping" % container)
                continue

            self.log.debug("Meta: %s" % meta)

            try:
                object_size = int(meta['object-size'])
                objects = int(meta['objects'])
            except ValueError as ex:
                self.log.error("%s doesn't appear to be correct: %s" % (container, ex))
                return 1

            if meta['version'] != disk_version:
                self.log.warning("Version mismatch %s != %s in %s" % (meta['version'], disk_version, container))

            stores[container] = SwiftStorage(values['authurl'],
                                             values['username'],
                                             values['password'],
                                             container,
                                             object_size,
                                             objects,
                                             Cache(int(self.args.cache_limit*1024**2 / object_size)),
                                             values['read-only'].lower() in ('1', 'yes', 'true', 'on'),
                                            )

        addr = (self.args.bind_address, self.args.bind_port)
        server = Server(addr, stores)

        gevent.signal(signal.SIGTERM, server.stop)
        gevent.signal(signal.SIGINT, server.stop)

        if not self.args.foreground:
            try:
                if gevent.fork() != 0:
                    os._exit(0)
            except OSError as ex:
                self.log.error("Failed to daemonize: %s" % ex)
                return 1

            os.setsid()
            fd = os.open(os.devnull, os.O_RDWR)
            os.dup2(fd, sys.stdin.fileno())
            os.dup2(fd, sys.stdout.fileno())
            os.dup2(fd, sys.stderr.fileno())

            gevent.reinit()

        self.log.info("Starting to serve on %s:%s" % (addr[0], addr[1]))

        try:
            fd = os.open(self.args.pidfile, (os.O_CREAT|os.O_EXCL|os.O_WRONLY), 0644)
        except OSError as ex:
            self.log.error("Failed to create the pidfile: %s" % ex)
            return 1

        with os.fdopen(fd, "w") as pidfile_handle:
            pidfile_handle.write("%s\n" % os.getpid())
            pidfile_handle.flush()

            server.serve_forever()

        os.remove(self.args.pidfile)

        # unlock the storages before exit
        server.unlock_all()

        self.log.info("Exiting...")
        return 0

