#!/usr/bin/env python3

from base64 import b64encode
from datetime import datetime
from tarfile import BLOCKSIZE, TarInfo, EOFHeaderError
import gzip
import hashlib
import json
import struct
import zlib
import sys


ENCODING = "utf-8"


def read_entries(f):
    """
    read entries and payloads from a tarball fileobj, each yield block
    of bytes includes the tarinfo entry (1 BLOCKSIZE) and the following
    payload inclusive of padding (1 or more BLOCKSIZE).
    """
    offset = 0
    while True:
        try:
            header_buf = f.read(BLOCKSIZE)
            header = TarInfo.frombuf(header_buf, encoding=ENCODING,
                                     errors="surrogateescape")
            header.offset = offset
            blocks, remainder = divmod(header.size, BLOCKSIZE)
            # Pad until the next block
            payload_blocks = blocks + int(remainder > 0)
            payload = f.read(BLOCKSIZE * payload_blocks)
            yield header, header_buf + payload
            offset += BLOCKSIZE * (1 + payload_blocks)
        except EOFHeaderError:
            return


def index_state_chunks(entries):
    header, block = next(entries)

    num_entries = 0
    index_state = header.mtime
    buf = bytearray(block)

    for n in entries:
        num_entries += 1
        header, block = n
        if header.mtime > index_state:
            # yield current chunk
            yield index_state, num_entries, buf
            # start new chunk
            num_entries = 0
            index_state = header.mtime
            buf = bytearray()
        buf += block

    yield index_state, num_entries, buf


infn = '/home/andrea/.cabal/packages/hackage.haskell.org/01-index.tar.gz'
outfn = 'out-01-index.tar.gz'

with gzip.GzipFile(filename=infn, mode='rb') as fin:

    entries = read_entries(fin)
    chunks = index_state_chunks(entries)

    original_size = 0

    # write a fixed header to match hackage this is never going to change
    # anyway
    header = bytes.fromhex('1f8b0800000000000203')

    # initialise compression object with the same settings as hackage
    compress = zlib.compressobj(9,
                                zlib.DEFLATED,
                                -zlib.MAX_WBITS,
                                zlib.DEF_MEM_LEVEL,
                                0)

    # initialise crc calculation (NOTE this is of the uncompressed data)
    crc = zlib.crc32(b"")

    # initialise hash calculation (NOTE this is of the compressed data)
    sha256 = hashlib.sha256(header)

    with open(outfn, mode='wb') as fout:
        fout.write(header)

        for index_state, _, block in chunks:
            compressed_block = compress.compress(block)
            original_size += len(block)
            crc = zlib.crc32(block, crc)

            fout.write(compressed_block)
            sha256.update(compressed_block)

            # prefix_size is what we have been able to flush to disk already
            prefix_size = fout.tell()

            # here we need to compose the trailing part of the tgz
            # which includes
            # 1. adding the two zeros BLOCKSIZE to mark the end of the tarball
            # 2. flushing the remaining bytes in the compression objects
            # 3. the trailer of the gzip

            # tar trailer (before compression)
            compress_copy = compress.copy()

            eof_marker = b'\0' * (BLOCKSIZE * 2)

            trailer = compress_copy.compress(eof_marker)
            trailer_original_size = original_size + len(eof_marker)
            trailer_crc = zlib.crc32(eof_marker, crc)

            trailer += compress_copy.flush()

            # this is the gzip trailer (after compression)
            trailer += struct.pack("<L", trailer_crc)
            trailer += struct.pack("<L", trailer_original_size & 0xffffffff)

            trailer_sha256 = sha256.copy()
            trailer_sha256.update(trailer)
            trailer_sha256_digest = trailer_sha256.hexdigest()

            index_state_str = \
                datetime.utcfromtimestamp(index_state).isoformat() + 'Z'

            # trailer also includes the crc and the size
            print(json.dumps({
                "key": index_state_str,
                "value": b64encode(trailer).decode(),
                "base64": True,
                "metadata": {
                    "prefix_size": prefix_size,
                    "sha256": trailer_sha256_digest
                    }}))

        fout.write(trailer)
