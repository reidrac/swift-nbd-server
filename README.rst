================
Swift NBD Server
================

This is a Network Block Device (NBD) server for OpenStack Object Storage (Swift).

Very often users want to run tools like rsync on top of Swift, but this is not
possible because the object storage HTTP API can't provide a file system alike
functionality. This project aims to support a block interface for the object
storage via NBD.


How it Works
============

**swiftnbd** translates the NBD requests (read/write with offset and length) to Swift object
operations, as displayed in the following picture:

.. image:: https://github.com/reidrac/swift-nbd-server/raw/master/block2object.png

Although this strategy may work with any block interface, NBD was chosen because of its simplicity.
The NBD server can serve the blocks over the network, but is recommended that it is used locally.
Because the communication with Swift will be the bottleneck, the possible overhead of NBD on localhost
is expected to not be significant.

The block device can be used only by one location at once. When a client is connected to the server,
the container used as storage is marked as *locked* by adding metadata information to the container
until the client disconnects and the container can be unlocked.

References:

- OpenStack Object Storage: http://www.openstack.org/software/openstack-storage/
- NBD: http://nbd.sourceforge.net/
- NBD protocol: https://github.com/yoe/nbd/blob/master/doc/proto.txt
- NBD server example in Python: http://lists.canonical.org/pipermail/kragen-hacks/2004-May/000397.html


Install
=======

Requirements:

- Linux (or any platform with NBD client)
- Python 2.7 (or later; Python 3 not supported yet)
- python-swiftclient
- gevent

To install the software, run the following command::

    python setup.py install

Alternatively you can install it with pip::

    pip install swiftnbd


Usage
=====

A container needs to be setup with swiftnbd-ctl to be used by the server. First create
a *secrets.conf* file::

    [container-name]
    username = user
    password = pass

Optionally an *authurl* token can be used to specify an authentication URL per container.

The default location for the *secrets* is */etc/swiftnbd/secrets.conf*, and an alternative
location can be provided using *--secrets* flag.

Then run the control tool with *setup* command using the container name as first parameter
and the maximum number of objects you want to allocate as second parameter::

    swiftnbd-ctl setup container-name number-of-objects

For example, setup a 1GB storage in myndb0 container::

    swiftnbd-ctl setup mynbd0 16384

By default the objects stored in swift by the server are 64KB, so 16384 * 65536 is 1GB.

After the container is setup, it can be served with swiftnbd-server::

    swiftnbd-server container-name

For debugging purposes the *-vf* flag is recommended (verbose and foreground).

The server implements a local cache that by default is limited to 64 MB. That value can
be configured using the *-c* flag indicating the max amount of memory to be used (in MB).

Once the server is running, nbd-client can be used to create the block device (as root)::

    modprobe nbd
    nbd-client 127.0.0.1 10811 /dev/nbd0

Then */dev/nbd0* can be used as a regular block device, ie::

    mkfs.ext3 /dev/nbd0
    mount /dev/nbd0 /mnt

Before stopping the server, be sure you unmount the device and stop the NBD client::

    umount /mnt
    nbd-client -d /dev/nbd0

Please check *--help* for further details.


Control tool
------------

siwftnbd-ctl is used to perform different maintenance operations on the containers. It
communicates directly with the object storage.

To list the containers::

    swiftnbd-ctl list -s

To setup a container::

    swiftnbd-ctl setup container-name number-of-objects

A custom object size can be indicated with the *--object-size* flag (default is 65536).

To unlock a locked container::

    swiftnbd-ctl unlock container-name

To download a container into a local disk image (the resulting disk image can be
mounted using a loop device)::

    swiftnbd-ctl download container-name image-file.raw

To delete a container (all the objects in the container will deleted before deleting
the container)::

    swiftnbd-ctl delete container-name


Known issues and limitations
============================

- The default 64KB object size is a wild/random guess, other values could be better.
- It can be used over the Internet but the performance is dependant on the bandwidth, so
  it's recommended that the storage is accessible via LAN (or same data center with 100 mbps
  or better).


License
=======

This is free software under the terms of MIT license (check COPYING file
included in this package).


Contact and support
===================

The project website is at: https://github.com/reidrac/swift-nbd-server

There you can file bug reports, ask for help or contribute patches.


Author
======

- Juan J. Martinez <jjm@usebox.net>

