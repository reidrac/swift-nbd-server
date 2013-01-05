#!/usr/bin/env python
"""
swiftnbd. setup module
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
from argparse import ArgumentParser

from swiftclient import client

from swiftnbd.const import version, project_url, auth_url, object_size, secrets_file, disk_version
from swiftnbd.common import setLog, setMeta, getMeta, getSecrets

class Main(object):
    def __init__(self):
        parser = ArgumentParser(description="Setup a OpenStack Object Storage container to be used by swiftnbd-server.",
                                epilog="Contact and support: %s" % project_url
                                )

        parser.add_argument("container", help="container used as storage")
        parser.add_argument("objects", help="number of objects")

        parser.add_argument("--version", action="version", version="%(prog)s "  + version)

        parser.add_argument("--secrets", dest="secrets_file",
                            default=secrets_file,
                            help="filename containing user/password (default: %s)" % secrets_file)

        parser.add_argument("-a", "--auth-url", dest="authurl",
                            default=auth_url,
                            help="default authentication URL (default: %s)" % auth_url)

        parser.add_argument("--object-size", dest="object_size",
                            default=object_size,
                            help="object size (default: %s)" % object_size)

        parser.add_argument("-f", "--force", dest="force",
                            action="store_true",
                            help="force operation")

        parser.add_argument("-v", "--verbose", dest="verbose",
                            action="store_true",
                            help="enable verbose logging"
                            )

        self.args = parser.parse_args()

        self.log = setLog(debug=self.args.verbose)

        try:
            self.username, self.password, self.authurl = getSecrets(self.args.container, self.args.secrets_file)
        except ValueError as ex:
            parser.error(ex)

        if self.authurl is None:
            self.authurl = self.args.authurl

    def run(self):

        cli = client.Connection(self.authurl, self.username, self.password)

        try:
            headers, _ = cli.get_container(self.args.container)
        except (socket.error, client.ClientException) as ex:
            if getattr(ex, 'http_status', None) == 404:
                self.log.debug("%s doesn't exist, will be created" % self.args.container)
            else:
                self.log.error(ex)
                return 1
        else:
            self.log.debug(headers)

            meta = getMeta(headers)
            self.log.debug("Meta: %s" % meta)

            if meta and not self.args.force:
                self.log.error("%s has already been setup" % self.args.container)
                return 1

            self.log.warning("%s has already been setup, forcing operation" % self.args.container)

        hdrs = setMeta(dict(version=disk_version, objects=self.args.objects, object_size=self.args.object_size, client='', last=''))
        self.log.debug("Meta headers: %s" % hdrs)

        try:
            cli.put_container(self.args.container, headers=hdrs)
        except client.ClientException as ex:
            self.log.error(ex)
            return 1

        self.log.info("Done, %s" % self.args.container)

        return 0

