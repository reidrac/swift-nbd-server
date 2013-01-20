Object size tests
=================

The tests have been performed over the Internet with a ~19 Mbps (down) and ~1.9
Mbps (up) connection and swiftnbd 0.9.2. This is a quite extreme use case, in a
production environment 100 Mbps or better is recommended. The time measurement
is provided as a comparative figure between the different object sizes.

All the tests start with an empty cache (64 MB default limit) and an empty
container.

Cases:

a. 64 KB object size, 1024 objects (64 MB disk).
b. 4 KB object size, 16384 objects (64 MB disk).

Tests:

1. mkfs.ext4 /dev/nbd0 ; sync
2. mount /dev/mbd0 /mnt ; sync ; ls /mnt


Results
-------

Test 1:

a. time 0m28.235s, in=4556800 (7471104), out=19456 (0), cache size=4980736, limit=67108864 (7.42%)
b. time 5m20.040s, in=4556800 (4694016), out=19456 (0), cache size=4579328, limit=67108864 (6.82%)

.. image:: https://raw.github.com/reidrac/swift-nbd-server/master/doc/obj-size-case-a.png

Test 2:

a. time 0m3.208s, in=2124800 (4980736), out=344064 (524288), cache size=2555904, limit=67108864 (3.81%)
b. time 0m4.375s, in=533504 (573440), out=344064 (208896), cache size=733184, limit=67108864 (1.09%)

.. image:: https://raw.github.com/reidrac/swift-nbd-server/master/doc/obj-size-case-b.png


Conclusions
-----------

- Using a larger object size minimises the number of PUT/GET requests to be performed, 
  decreasing the HTTP overhead, and in consequence the block device shows better
  performance.
- The bandwidth usage is higher with large objects because of the required padding of
  the requests.
- The cache is important to minimize the number of GET requests.
- Depending on the available bandwidth, I would suggest experimenting with larger blocks.


Jan 20th, 2013

