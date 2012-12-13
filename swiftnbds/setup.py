#!/usr/bin/env python
"""
swiftnbds. setup module
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

from swiftclient import client

from swiftnbds.const import version, project_url, auth_url, block_size, secrets_file
from swiftnbds.common import setLog, setMeta, getMeta, getSecrets

class Main(object):
    def __init__(self):
        parser = ArgumentParser(description="Setup a OpenStack Object Storage container to be used by swiftnbds.",
                                epilog="Contact and support: %s" % project_url
                                )

        parser.add_argument("container", help="container used as storage")
        parser.add_argument("blocks", help="number of blocks")

        parser.add_argument("--version", action="version", version="%(prog)s "  + version)

        parser.add_argument("--secrets", dest="secrets_file",
                            default=secrets_file,
                            help="filename containing user/password (default: %s)" % secrets_file)

        parser.add_argument("-a", "--auth-url", dest="authurl",
                            default=auth_url,
                            help="authentication URL (default: %s)" % auth_url)

        parser.add_argument("--block-size", dest="block_size",
                            default=block_size,
                            help="block size (default: %s)" % block_size)

        parser.add_argument("-f", "--force", dest="force",
                            action="store_true",
                            help="force operation")

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

        hdrs = setMeta(dict(version=version, blocks=self.args.blocks, block_size=self.args.block_size))
        self.log.debug("Meta headers: %s" % hdrs)

        try:
            cli.put_container(self.args.container, headers=hdrs)
        except client.ClientException as ex:
            self.log.error(ex)
            return 1

        self.log.info("Done, %s" % self.args.container)

        return 0

