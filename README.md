Swift NBD Server (swiftnbds)
============================

This is a Network Block Device (NBD) server for OpenStack Object Storage (Swift).

References:

 - http://nbd.sourceforge.net/
 - https://github.com/yoe/nbd/blob/master/doc/proto.txt
 - http://lists.canonical.org/pipermail/kragen-hacks/2004-May/000397.html

**Warning**: this is a proof of concept and in its current state is alpha quality.


Install
-------

Requirements:

 - python 2.7 (or later)
 - python-swiftclient
 - geventlet

To install the software, run the following command:

    python setup.py install


Usage
-----

A container needs to be setup with swiftnbd-setup to be used by the server:

    swiftbdd-setup username password container number-of-blocks

For example, setup a 1GB storage in myndb0 container:

    swiftbdd-setup user pass myndb0 16384

(by default block size is 8KB, so 16384 * 65536 is 1GB)

After the container is setup, it can be served with swiftnbds:

    swiftnbds username password container

Then you can use nbd-client to create the block device (as root):

    modprobe nbd
    nbd-client 127.0.0.1 10811 /dev/nbd0

Now just use /dev/nbd0 as a regular block device, ie:

    mkfs.ext2 /dev/nbd0
    mount /dev/nbd0 /mnt


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

