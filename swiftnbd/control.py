#!/usr/bin/env python
"""
swiftnbd. control module
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

import socket
import argparse
from argparse import ArgumentParser

from swiftclient import client

from swiftnbd.const import version, project_url, auth_url, object_size, secrets_file, disk_version
from swiftnbd.common import setLog, setMeta, getMeta, getSecrets, getContainers

class Main(object):

    def __init__(self):
        parser = ArgumentParser(description="swiftnbd control tool.",
                                epilog="Contact and support: %s" % project_url
                                )

        subp = parser.add_subparsers(title="commands")

        p = subp.add_parser('list', help='list all containers and their information')
        p.set_defaults(func=self.do_list)

        p = subp.add_parser('unlock', help='unlock a container')
        p.add_argument("container", help="container to unlock")
        p.set_defaults(func=self.do_unlock)

        parser.add_argument("--version", action="version", version="%(prog)s "  + version)

        parser.add_argument("--secrets", dest="secrets_file",
                            default=secrets_file,
                            help="filename containing user/password (default: %s)" % secrets_file)

        parser.add_argument("-a", "--auth-url", dest="authurl",
                            default=auth_url,
                            help="default authentication URL (default: %s)" % auth_url)

        parser.add_argument("-v", "--verbose", dest="verbose",
                            action="store_true",
                            help="enable verbose logging"
                            )

        self.args = parser.parse_args()

        self.log = setLog(debug=self.args.verbose)

    def run(self):
        return self.args.func()

    def do_list(self):

        self.log.debug("listing containers")

        containers = getContainers(self.args.secrets_file)

        prev_authurl = None
        for cont in containers:
            username, password, authurl = getSecrets(cont, self.args.secrets_file)

            if authurl is None:
                authurl = self.args.authurl

            if prev_authurl or prev_authurl != authurl:
                cli = client.Connection(authurl, username, password)

            try:
                headers, _ = cli.get_container(cont)
            except (socket.error, client.ClientException) as ex:
                if getattr(ex, 'http_status', None) == 404:
                    self.log.error("%s doesn't exist (auth-url: %s)" % (cont, authurl))
                else:
                    self.log.error(ex)
            else:

                meta = getMeta(headers)
                self.log.debug("Meta: %s" % meta)

                if meta:
                    lock = "unlocked" if not 'client' in meta else "locked by %s" % meta['client']
                    self.log.info("%s objects=%s size=%s (version=%s, %s)" % (cont,
                                                                              meta['objects'],
                                                                              meta['object-size'],
                                                                              meta['version'],
                                                                              lock,
                                                                              ))
                else:
                    self.log.info("%s is not a swiftnbd container" % cont)

            prev_authurl = authurl

        return 0

    def do_unlock(self):

        self.log.debug("unlocking %s" % self.args.container)

        try:
            self.username, self.password, self.authurl = getSecrets(self.args.container, self.args.secrets_file)
        except ValueError as ex:
            self.log.error(ex)
            return 1

        if self.authurl is None:
            self.authurl = self.args.authurl

        cli = client.Connection(self.authurl, self.username, self.password)

        try:
            headers, _ = cli.get_container(self.args.container)
        except (socket.error, client.ClientException) as ex:
            if getattr(ex, 'http_status', None) == 404:
                self.log.error("%s doesn't exist" % self.args.container)
            else:
                self.log.error(ex)
            return 1

        self.log.debug(headers)

        meta = getMeta(headers)
        self.log.debug("Meta: %s" % meta)

        if not meta:
            self.log.error("%s hasn't been setup to be used with swiftnbd" % self.args.container)
            return 1
        elif 'client' not in meta:
            self.log.warning("%s is not locked, nothing to do" % self.args.container)
            return 0

        self.log.info("%s lock is: %s" % (self.args.container, meta['client']))

        meta.update(client='', last=meta['client'])
        hdrs = setMeta(meta)
        self.log.debug("Meta headers: %s" % hdrs)

        try:
            cli.put_container(self.args.container, headers=hdrs)
        except client.ClientException as ex:
            self.log.error(ex)
            return 1

        self.log.info("Done, %s is unlocked" % self.args.container)

        return 0

