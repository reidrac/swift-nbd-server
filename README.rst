Swift NBD Server
================

This is a Network Block Device (NBD) server for OpenStack Object Storage (Swift).

Very often users want to run tools like rsync on top of Swift, but this is not
possible because the object storage HTTP API can't provide a file system alike
functionality. This project aims to support a block interface for the object
storage via NBD. 

*swiftnbd* translates the NBD requests (read/write with offset and length) to Swift object
operations, as displayed in the following picture:

![block to object translation](https://github.com/reidrac/swift-nbd-server/raw/master/block2object.png)

Although this strategy may work with any block interface, NBD was chosen because of its simplicity.
The NBD server can serve the blocks over the network, but I recommend it's used locally. Because the
communication with Swift will be the bottleneck, I expect the possible overhead of NBD on localhost
to not be significant.

References:

- OpenStack Object Storage: http://www.openstack.org/software/openstack-storage/
- NBD: http://nbd.sourceforge.net/
- NBD protocol: https://github.com/yoe/nbd/blob/master/doc/proto.txt
- NBD server example in Python: http://lists.canonical.org/pipermail/kragen-hacks/2004-May/000397.html


Install
-------

Requirements:

- Linux (or any platform with NBD client)
- Python 2.7 (or later; Python 3 not supported yet)
- python-swiftclient
- gevent

To install the software, run the following command:

    python setup.py install

Alternatively you can install it with pip:

    pip install swiftnbd


Usage
-----

A container needs to be setup with swiftnbd-setup to be used by the server. First create
a secrets.conf file:

    [container-name]
    username = user
    password = pass

Then run the setup tool using the container name as first parameter:

    swiftnbd-setup container-name number-of-objects

For example, setup a 1GB storage in myndb0 container:

    swiftnbd-setup mynbd0 16384 --secrets secrets.conf

Notes:

- by default the objects stored in swift are 64KB, so 16384 * 65536 is 1GB
- swiftnbd-setup can be used to unlock a storage using the -f flag to overwrite the
  container metadata (as long as the number-of-objects is the same, it won't affect
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

Before stopping the server, be sure you unmount the device and stop the nbd client:

    umount /mnt
    nbd-client -d /dev/nbd0

Please check --help for further details.


Known issues and limitations
----------------------------

- The default 64KB storage block is a wild/random guess, other values could be better.
- The storage can't be mounted in more than one client at once (there's a lock mechanism
  for that).
- It can be used over the Internet but the performance is dependant on the bandwidth, so
  it's recommended that the storage is accessible via LAN (or same data center with 100mbps
  or better).


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

