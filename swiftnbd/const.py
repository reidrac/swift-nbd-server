#!/usr/bin/env python
"""
swiftnbd. constants
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
version = "0.9.7"
description = "This is a NBD server for OpenStack Object Storage (Swift)."
project_url = "https://github.com/reidrac/swift-nbd-server"

# OpenStack Object Storage implementation used as reference
# Memstore: http://www.memset.com/cloud/storage/
auth_url = "https://auth.storage.memset.com/v1.0"

# size of the objects
object_size = 1024*64

# user/password information for each container
secrets_file = "/etc/swiftnbd/secrets.conf"

# for disk format versioning
disk_version = "1"

# stats delay (seconds)
stats_delay = 300

