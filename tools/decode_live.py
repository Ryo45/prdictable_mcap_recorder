#!/usr/bin/env python3
"""Independent streaming reader for unchunked live MCAP and its source codecs."""
import argparse
from functools import cache
import ctypes
import ipaddress
import json
import struct
from pathlib import Path
import zlib

MAGIC = b"\x89MCAP0\r\n"
LIMIT = 32 << 20


def unpack_strings(data, start, count):
    values = []
    for _ in range(count):
        size, = struct.unpack_from("<I", data, start)
        start += 4
        end = start + size
        if end > len(data):
            raise ValueError("truncated MCAP string")
        values.append(data[start:end].decode())
        start = end
    return values, start


@cache
def load_zstd():
    library = ctypes.CDLL("libzstd.so.1")
    library.ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t,
                                        ctypes.c_void_p, ctypes.c_size_t]
    library.ZSTD_decompress.restype = ctypes.c_size_t
    library.ZSTD_isError.argtypes = [ctypes.c_size_t]
    library.ZSTD_isError.restype = ctypes.c_uint
    return library


def decode(encoding, payload):
    if encoding == "cdr":
        return {}, payload
    if len(payload) < 56 or payload[0] != 1:
        raise ValueError("invalid envelope")
    version, transform, a, b, length, interface = struct.unpack_from("<BBBBII", payload)
    if length > LIMIT:
        raise ValueError("invalid decoded size")
    body = payload[56:]
    if transform == 1:
        zstd = load_zstd()
        output = ctypes.create_string_buffer(max(length, 1))
        result = zstd.ZSTD_decompress(output, length, body, len(body))
        if zstd.ZSTD_isError(result) or result != length:
            raise ValueError("invalid Zstd payload")
        body = output.raw[:length]
    if transform not in (0, 1) or len(body) != length:
        raise ValueError("invalid transform or payload size")
    info = dict(version=version, transform=("identity", "zstd")[transform], interface_index=interface)
    if encoding == "recorder-udp-v1":
        def addr(family, offset):
            if family == 0:
                return None
            return str(ipaddress.ip_address(payload[offset:offset + {4: 4, 6: 16}[family]]))
        source_port, dst_port, scope = struct.unpack_from("<HHI", payload, 12)
        info.update(source=addr(a, 24), destination=addr(b, 40), source_port=source_port,
                    destination_port=dst_port, source_scope=scope)
    elif encoding == "recorder-socketcan-v1":
        can_id, = struct.unpack_from("<I", payload, 12)
        info.update(raw_can_id=can_id, can_id=can_id & (0x1fffffff if can_id & 0xa0000000 else 0x7ff),
                    fd=bool(a), eff=bool(can_id & 0x80000000), rtr=bool(can_id & 0x40000000),
                    error=bool(can_id & 0x20000000), fd_flags=b, brs=bool(b & 1), esi=bool(b & 2),
                    dlc=payload[16] if payload[16] != 255 else None)
    else:
        raise ValueError(f"unknown encoding {encoding}")
    return info, body


def messages(path):
    with path.open("rb") as file:
        if file.read(8) != MAGIC:
            raise ValueError("bad initial magic")
        crc = zlib.crc32(MAGIC)
        channels = {}
        data_end = False
        while prefix := file.read(9):
            op, length = struct.unpack("<BQ", prefix)
            if length > LIMIT:
                raise ValueError("record too large")
            data = file.read(length)
            if len(data) != length:
                raise ValueError("truncated record")
            if op == 0x0f:
                if struct.unpack("<I", data)[0] != crc:
                    raise ValueError("MCAP data CRC mismatch")
                data_end = True
            elif op == 0x02:
                if not data_end or file.read(8) != MAGIC or file.read(1):
                    raise ValueError("invalid MCAP tail")
                return
            elif data_end:
                raise ValueError("unexpected post-DataEnd record")
            elif op == 0x04:
                identifier, _ = struct.unpack_from("<HH", data)
                (topic, encoding), offset = unpack_strings(data, 4, 2)
                size, = struct.unpack_from("<I", data, offset)
                offset += 4
                stop = offset + size
                metadata = {}
                while offset < stop:
                    (key, value), offset = unpack_strings(data, offset, 2)
                    metadata[key] = value
                if offset != stop or stop != len(data):
                    raise ValueError("bad channel metadata")
                channels[identifier] = (topic, encoding, metadata)
            elif op == 0x05:
                channel, sequence, log_time, publish_time = struct.unpack_from("<HIQQ", data)
                topic, encoding, metadata = channels[channel]
                info, body = decode(encoding, data[22:])
                yield dict(topic=topic, encoding=encoding, sequence=sequence, log_time=log_time,
                           publish_time=publish_time, channel=metadata, **info), body
            crc = zlib.crc32(data, zlib.crc32(prefix, crc))
        raise ValueError("missing MCAP footer")


def candump(info, payload):
    if info["encoding"] != "recorder-socketcan-v1":
        return None
    timestamp = f"{info['log_time'] // 10**9}.{info['log_time'] % 10**9 // 1000:06}"
    identifier = info["raw_can_id"] & (0x3fffffff if info["error"] else 0x1fffffff)
    can_id = f"{identifier:08X}" if info["eff"] or info["error"] else f"{identifier:03X}"
    frame = (f"##{info['fd_flags'] & 0xf:X}{payload.hex().upper()}" if info["fd"]
             else f"#R{info['dlc']:X}" if info["rtr"] else f"#{payload.hex().upper()}")
    return f"({timestamp}) {info['channel']['interface']} {can_id}{frame}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--candump", action="store_true")
    args = parser.parse_args()
    for info, payload in messages(args.input):
        if args.candump:
            if line := candump(info, payload):
                print(line)
        else:
            print(json.dumps(info | {"payload_hex": payload.hex()}))


if __name__ == "__main__":
    main()
