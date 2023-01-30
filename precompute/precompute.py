from base64 import b64encode
from datetime import datetime
from tarfile import BLOCKSIZE, TarInfo
import gzip
import hashlib
import struct
import zlib

ENCODING = "utf-8"


def read_entries(f):
    """
    read entries and payloads from a tarball fileobj, each yield block
    of bytes includes the tarinfo entry (1 BLOCKSIZE) and the following
    payload inclusive of padding (1 or more BLOCKSIZE).
    """
    offset = 0
    while True:
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


def index_state_chunks(entries):
    header, block = next(entries)

    num_entries = 0
    index_state = header.mtime
    buf = bytearray(block)

    while n := next(entries):
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

    # max_blocks
    MAX_CHUNKS = 1
    num_chunks = 0

    with open(outfn, mode='wb') as fout:

        fout.write(header)

        for index_state, _, block in chunks:
            if num_chunks >= MAX_CHUNKS:
                break
            num_chunks += 1

            original_size += len(block)

            compressed_block = compress.compress(block)

            fout.write(compressed_block)

            # prefix_size is what we have been able to flush to disk already
            prefix_size = fout.tell()

            prev_crc = crc
            crc = zlib.crc32(block, prev_crc)
            sha256.update(compressed_block)

            # here we need to compose the trailing part of the tgz
            # which includes
            # 1. flushing the remaining bytes in the compression objects
            # 2. the trailer of the gzip

            trailer = compress.copy().flush()

            trailer += struct.pack("<L", crc)
            trailer += struct.pack("<L", original_size & 0xffffffff)

            trailer_sha256 = sha256.copy()
            trailer_sha256.update(trailer)
            trailer_sha256_digest = trailer_sha256.hexdigest()

            index_state_str = \
                datetime.utcfromtimestamp(index_state).isoformat() + 'Z'

            # trailer also includes the crc and the size
            print(index_state_str,
                  "prefix_size", fout.tell(),
                  "trailer size", len(trailer),
                  "trailer sha256", trailer_sha256_digest,
                  "trailer", b64encode(trailer).decode())

        fout.write(trailer)
