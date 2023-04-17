_VERSION_HASH_FILE = "version.sha256"

with open(_VERSION_HASH_FILE) as f:
    sha256sum = f.read(64)
