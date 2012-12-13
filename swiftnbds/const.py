version = "pre-0.1"
description = "This is a NBD server for OpenStack Object Storage (Swift)."
project_url = "https://github.com/reidrac/swift-nbd-server"

# OpenStack Object Storage implementation used as reference
# Memstore: http://www.memset.com/cloud/storage/
auth_url = "https://auth.storage.memset.com/v1.0"

# size of the blocks
block_size = 1024*64

# user/password information for each container
secrets_file = "/etc/swiftnbd/secrets.conf"

