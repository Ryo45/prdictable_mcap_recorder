#!/usr/bin/env python3
"""One configured ROS 2 raw subscription. stdout is a length-framed binary stream."""
import os
from pathlib import Path
import re
import struct
import sys

import rclpy
from ament_index_python.packages import get_package_share_directory
from rclpy.qos import QoSProfile, qos_profile_sensor_data
from rosidl_runtime_py.utilities import get_message


def definitions(message_type):
    seen = set()
    sections = []

    def visit(kind):
        if kind in seen:
            return
        seen.add(kind)
        package, namespace, name = kind.split("/")
        if namespace != "msg":
            raise ValueError(f"unsupported interface: {kind}")
        path = Path(get_package_share_directory(package)) / "msg" / f"{name}.msg"
        body = path.read_text(encoding="utf-8")
        sections.append((kind, body))
        for line in body.splitlines():
            field = line.split("#", 1)[0].split()
            if not field:
                continue
            token = field[0].split("[", 1)[0]
            if token == "Header":
                visit("std_msgs/msg/Header")
            elif "/" in token:
                parts = token.split("/")
                visit("/".join(parts if len(parts) == 3 else (parts[0], "msg", parts[1])))
            elif re.fullmatch(r"[A-Z][A-Za-z0-9_]*", token):
                visit(f"{package}/msg/{token}")

    visit(message_type)
    return (sections[0][1].rstrip() + "\n" + "".join(
        "\n" + "=" * 80 + f"\nMSG: {kind}\n{body.rstrip()}\n"
        for kind, body in sections[1:])).encode()


def main():
    topic, message_type, qos, bound = sys.argv[1:]
    bound = int(bound)
    schema = definitions(message_type)
    msg = get_message(message_type)
    if not 0 < len(schema) <= 1024 * 1024:
        raise ValueError("schema exceeds bridge bound")
    # Keep binary stdout separate from any middleware diagnostics.
    pipe = os.fdopen(os.dup(1), "wb", buffering=0)
    os.dup2(2, 1)

    def write_all(data):
        view = memoryview(data)
        while view:
            count = pipe.write(view)
            if not count:
                raise BrokenPipeError("raw subscription pipe closed")
            view = view[count:]

    def callback(serialized, message_info):
        length = len(serialized)
        stamp = max(0, int(message_info.get("source_timestamp", 0)))
        write_all(struct.pack("<IQ", length if length <= bound else 0xffffffff, stamp))
        if length <= bound:
            write_all(serialized)

    rclpy.init(args=[])
    node = rclpy.create_node(f"recorder_raw_{os.getpid()}")
    profile = qos_profile_sensor_data if qos == "sensor-data" else QoSProfile(depth=10)
    node.create_subscription(msg, topic, callback, profile, raw=True)
    write_all(struct.pack("<IQ", len(schema), 0))
    write_all(schema)
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
