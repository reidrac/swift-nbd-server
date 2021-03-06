#!/usr/bin/env python
"""
swiftnbd. control module
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

import socket
import sys
from time import time
from argparse import ArgumentParser

from swiftclient import client

from swiftnbd.const import (version, description, project_url, auth_url, secrets_file, object_size,
        disk_version, keystone_separator, keystone_service, keystone_endpoint)
from swiftnbd.common import setLog, setMeta, getMeta, Config
from swiftnbd.swift import SwiftStorage, StorageError

class Main(object):

    def __init__(self):
        parser = ArgumentParser(description="swiftnbd control tool.",
                                epilog="Contact and support: %s" % project_url
                                )

        subp = parser.add_subparsers(title="commands")
        subp.required = True
        subp.dest = "command"

        p = subp.add_parser('list', help='list all containers and their information')
        p.add_argument("-s", "--simple-output", dest="simple",
                       action="store_true",
                       help="write simplified output to stdout")
        p.set_defaults(func=self.do_list)

        p = subp.add_parser('setup', help='setup a container to be used by the server')
        p.add_argument("container", help="container to setup")
        p.add_argument("objects", help="number of objects")

        p.add_argument("--object-size", dest="object_size",
                       default=object_size,
                       help="object size (default: %s)" % object_size)

        p.add_argument("-f", "--force", dest="force",
                       action="store_true",
                       help="force operation")

        p.set_defaults(func=self.do_setup)

        p = subp.add_parser('unlock', help='unlock a container')
        p.add_argument("container", help="container to unlock")
        p.set_defaults(func=self.do_unlock)

        p = subp.add_parser('lock', help='lock a container')
        p.add_argument("container", help="container to lock")
        p.set_defaults(func=self.do_lock)

        p = subp.add_parser('download', help='download a container as a raw image')
        p.add_argument("container", help="container to download")
        p.add_argument("image", help="local file to store the image")
        p.add_argument("-q", "--quiet", dest="quiet",
                       action="store_true",
                       help="don't show the process bar")
        p.set_defaults(func=self.do_download)

        p = subp.add_parser('delete', help='delete a container')
        p.add_argument("container", help="container to delete")
        p.set_defaults(func=self.do_delete)

        parser.add_argument("--version", action="version", version="%(prog)s "  + version)

        parser.add_argument("--secrets", dest="secrets_file",
                            default=secrets_file,
                            help="filename containing user/password (default: %s)" % secrets_file)

        parser.add_argument("-a", "--auth-url", dest="authurl",
                            default=auth_url,
                            help="default authentication URL (default: %s)" % auth_url)

        parser.add_argument("-k", "--keystone-auth", dest="keystone",
                            action="store_true",
                            help="use auth 2.0 (keystone, requires keystoneclient)")

        parser.add_argument("--keystone-separator", dest="keystone_separator",
                            default=keystone_separator,
                            help="tenant separator to be used with auth 2.0 (default: %s)" % keystone_separator)

        parser.add_argument("--keystone-service", dest="keystone_service",
                            default=keystone_service,
                            help="service to be used with auth 2.0 (default: %s)" % keystone_service)

        parser.add_argument("--keystone-endpoint", dest="keystone_endpoint",
                            default=keystone_endpoint,
                            help="endpoint to be used with auth 2.0 (default: %s)" % keystone_endpoint)

        parser.add_argument("--keystone-region", dest="keystone_region",
                            default=None,
                            help="region to be used with auth 2.0 (optional)")

        parser.add_argument("-v", "--verbose", dest="verbose",
                            action="store_true",
                            help="enable verbose logging"
                            )

        self.args = parser.parse_args()

        self.log = setLog(debug=self.args.verbose)

        try:
            self.conf = Config(self.args.secrets_file)
        except OSError as ex:
            parser.error("Failed to load secrets: %s" % ex)

        # setup by _setup_client()
        self.auth = None

    def run(self):
        return self.args.func()

    def do_list(self):

        self.log.debug("listing containers")

        if self.args.simple:
            out = print
        else:
            out = self.log.info

        for container, values in self.conf.items():
            cli, meta = self._setup_client(container=container)

            if cli:
                self.log.debug("Meta: %s" % meta)

                if meta:
                    lock = "unlocked" if not 'client' in meta else "locked by %s" % meta['client']
                    out("%s objects=%s size=%s (version=%s, %s)" % (container,
                                                                    meta['objects'],
                                                                    meta['object-size'],
                                                                    meta['version'],
                                                                    lock,
                                                                    ))
                else:
                    out("%s is not a swiftnbd container" % container)

        return 0

    def _setup_client(self, create=False, container=None):
        """
        Setup a client connection.
        If create is True and the container doesn't exist, it is created.

        Sets auth dictionary to create connections.

        Returns a client connection, metadata tuple or (None, None) on error.
        """

        if container is None:
            container = self.args.container

        try:
            values = self.conf.get_container(container)
        except ValueError as ex:
            self.log.error(ex)
            return (None, None)

        auth = dict(authurl = self.args.authurl,
                    user = values['username'],
                    key = values['password'],
                    )

        if self.args.keystone:
            try:
                from keystoneclient.v2_0 import client as _check_for_ksclient
            except ImportError:
                sys.exit("auth 2.0 (keystone) requires python-keystoneclient")
            else:
                self.log.debug("using auth 2.0 (keystone)")

            if self.args.keystone_separator not in values['username']:
                self.log.error("%s: separator not found in %r" % (container, values['username']))
                return (None, None)

            keystone_auth = values['username'].split(self.args.keystone_separator, 1)
            auth['tenant_name'], auth['user'] = keystone_auth
            auth['auth_version'] = '2.0'
            auth['os_options'] = dict(service_type = self.args.keystone_service,
                                      endpoint_type = self.args.keystone_endpoint,
                                      region_name = self.args.keystone_region,
                                      )
            self.log.debug("os_options: %r" % auth['os_options'])

        self.auth = auth
        cli = client.Connection(**auth)

        try:
            headers, _ = cli.get_container(container)
        except (socket.error, client.ClientException) as ex:
            if getattr(ex, 'http_status', None) == 404:
                if create:
                    self.log.warning("%s doesn't exist, will be created" % container)
                    return (cli, dict())
                else:
                    self.log.error("%s doesn't exist" % container)
            else:
                self.log.error(ex)
            return (None, None)

        self.log.debug(headers)

        meta = getMeta(headers)
        self.log.debug("Meta: %s" % meta)

        if not meta:
            self.log.error("%s hasn't been setup to be used with swiftnbd" % container)
            return (None, None)

        return (cli, meta)

    def do_unlock(self):

        self.log.debug("unlocking %s" % self.args.container)

        cli, meta = self._setup_client()
        if cli is None:
            return 1
        elif 'client' not in meta:
            self.log.warning("%s is not locked, nothing to do" % self.args.container)
            return 1

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

    def do_lock(self):

        self.log.debug("locking %s" % self.args.container)

        cli, meta = self._setup_client()
        if cli is None:
            return 1
        elif 'client' in meta:
            self.log.warning("%s is already locked: %s" % (self.args.container, meta['client']))
            return 1

        meta.update(client='ctl@%i' % time())
        hdrs = setMeta(meta)
        self.log.debug("Meta headers: %s" % hdrs)

        try:
            cli.put_container(self.args.container, headers=hdrs)
        except client.ClientException as ex:
            self.log.error(ex)
            return 1

        self.log.info("Done, %s is locked" % self.args.container)

        return 0

    def do_download(self):

        self.log.debug("downloading %s" % self.args.container)

        cli, meta = self._setup_client()
        if cli is None:
            return 1
        elif 'client' in meta:
            self.log.error("%s is locked, downloading a container in use is unreliable" % self.args.container)
            return 1

        object_size = int(meta['object-size'])
        objects = int(meta['objects'])

        store = SwiftStorage(self.auth,
                             self.args.container,
                             object_size,
                             objects,
                             )
        try:
            store.lock("ctl-download")
        except StorageError as ex:
            self.log.error(ex)
            return 1

        size = 0
        fdo = None
        try:
            fdo = open(self.args.image, "wb")

            while True:
                data = store.read(object_size)
                if data == b'':
                    break
                fdo.write(data)
                size += len(data)
                if not self.args.quiet:
                    sys.stdout.write("\rDownloading %s [%.2d%%]" % (self.args.container, 100*size/(objects*object_size)))
                    sys.stdout.flush()
        except IOError as ex:
            self.log.error(ex)
            return 1
        except KeyboardInterrupt:
            self.log.warning("user interrupt")
            return 1
        finally:
            if fdo:
                fdo.close()

            try:
                store.unlock()
            except StorageError as ex:
                self.log.warning("Failed to unlock %s: %s" % (self.args.container, ex))

        if not self.args.quiet:
            sys.stdout.write("\r")
            sys.stdout.flush()

        self.log.info("Done, %s bytes written" % size)

        return 0

    def do_delete(self):

        self.log.debug("deleting %s" % self.args.container)

        cli, meta = self._setup_client()
        if cli is None:
            return 1
        elif 'client' in meta:
            self.log.error("%s is locked" % self.args.container)
            return 1

        # this is the default limit for swift
        limit = 10000
        marker = None
        while True:
            try:
                _, objs = cli.get_container(self.args.container, limit=limit, marker=marker)
            except client.ClientException as ex:
                self.log.error(ex)
                return 1

            for obj in objs:
                if 'name' in obj:
                    try:
                        cli.delete_object(self.args.container, obj['name'])
                    except client.ClientException as ex:
                        self.log.error("Failed to delete %s: %s" % (obj['name'], ex))
                        return 1

            if len(objs) < limit:
                break
            else:
                marker = objs[-1]
                self.log.debug("More than %s files, marker=%s" % (limit, marker))

        try:
            cli.delete_container(self.args.container)
        except client.ClientException as ex:
            self.log.error("Failed to delete %s: %s" % (self.args.container, ex))
            return 1

        self.log.info("Done, %s has been deleted" % self.args.container)

        return 0

    def do_setup(self):

        self.log.debug("setting up %s" % self.args.container)

        cli, meta = self._setup_client(create=True)
        if cli is None:
            return 1
        elif meta and not self.args.force:
            self.log.error("%s has already been setup" % self.args.container)
            return 1

        hdrs = setMeta(dict(version=disk_version, objects=self.args.objects, object_size=self.args.object_size, client='', last=''))
        self.log.debug("Meta headers: %s" % hdrs)

        try:
            cli.put_container(self.args.container, headers=hdrs)
        except client.ClientException as ex:
            self.log.error(ex)
            return 1

        self.log.info("Done, %s" % self.args.container)

        return 0
