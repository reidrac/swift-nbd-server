Swift NBD Server
================

This is a Network Block Device (NBD) server for OpenStack Object Storage (Swift).

References:

 - http://nbd.sourceforge.net/
 - https://github.com/yoe/nbd/blob/master/doc/proto.txt
 - http://lists.canonical.org/pipermail/kragen-hacks/2004-May/000397.html

**Warning**: this is a work in progress and in current state is alpha quality.


Install
-------

Requirements:

 - python 2.7 (or later)
 - python-swiftclient
 - gevent

To install the software, run the following command:

    python setup.py install


Usage
-----

A container needs to be setup with swiftnbd-setup to be used by the server. First create
a secrets.conf file:

    [container-name]
    username = user
    password = pass

Then run the setup tool using the container name as first parameter:

    swiftnbd-setup container-name number-of-blocks

For example, setup a 1GB storage in myndb0 container:

    swiftnbd-setup mynbd0 16384 --secrets secrets.conf

Notes:

 - by default the blocks stored in swift are 64KB, so 16384 * 65536 is 1GB
 - swiftnbd-setup can be used to unlock a storage using the -f flag to overwrite the
   container metadata (as long as the number-of-blocks is the same, it won't affect
   the stored data); this is only until we have a specific tool for that

After the container is setup, it can be served with swiftnbdd:

    swiftnbdd container-name --secrets secrets.conf

Notes:

 - for debugging purposes, use -vf flag (verbose and foreground)

Then you can use nbd-client to create the block device (as root):

    modprobe nbd
    nbd-client 127.0.0.1 10811 /dev/nbd0

Now just use /dev/nbd0 as a regular block device, ie:

    mkfs.ext3 /dev/nbd0
    mount /dev/nbd0 /mnt

Please check --help for further details.


License
-------

This is free software under the terms of MIT license (check COPYING file
included in this package).


Contact and support
-------------------

The project website is at:

  https://github.com/reidrac/swift-nbd-server

There you can file bug reports, ask for help or contribute patches.


Author
------

 - Juan J. Martinez <jjm@usebox.net>

