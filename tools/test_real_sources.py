#!/usr/bin/env python3
"""Drive actual sources and compare recorded bytes with known sender inputs."""
import argparse
import json
import os
import signal
from pathlib import Path
import socket
import struct
import subprocess
import time

from decode_live import messages


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--recorder", type=Path, default=Path("target/release/recorder"))
    cli.add_argument("--out", type=Path, required=True)
    cli.add_argument("--mode", choices=("udp", "can", "ros", "mixed"), default="udp")
    cli.add_argument("--seconds", type=int, default=3)
    cli.add_argument("--writer", choices=("sync", "io-uring"), default="sync")
    cli.add_argument("--stop-early", action="store_true", help="send SIGINT after input completes")
    cli.add_argument("--interface", default="vcan0")
    args = cli.parse_args()
    if not 3 <= args.seconds <= 30:
        cli.error("use 3–30 seconds")
    args.out.mkdir(exist_ok=False)
    binary = args.recorder.resolve(strict=True)
    config = args.out / "config.toml"
    data = args.out / "run.mcap"
    parts = [f'[record]\nwriter="{args.writer}"\nqueue_capacity=128\ncommit_messages=32\nmax_output_bytes=67108864\n'
             '[durability]\nmode="overlap"\n']
    expected = {}
    udp = []
    can = None
    ros = None
    if args.mode in ("udp", "mixed"):
        for port_mode in ("identity", "zstd"):
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reserve:
                reserve.bind(("127.0.0.1", 0))
                port = reserve.getsockname()[1]
            sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sender.bind(("127.0.0.1", 0))
            udp.append((sender, port))
            parts.append(f'[[sources]]\nid="udp-{port_mode}"\nkind="udp"\nbind="127.0.0.1:{port}"\n'
                         f'max_payload_bytes=512\ntransform="{port_mode}"\n')
            expected[f"/udp/udp-{port_mode}"] = []
    if args.mode in ("can", "mixed"):
        can = socket.socket(socket.AF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
        can.setsockopt(socket.SOL_CAN_RAW, getattr(socket, "CAN_RAW_FD_FRAMES", 5), 1)
        can.bind((args.interface,))
        parts.append(f'[[sources]]\nid="can"\nkind="socketcan"\ninterface={json.dumps(args.interface)}\nenable_fd=true\n')
        expected["/socketcan/can"] = []
    if args.mode in ("ros", "mixed"):
        import rclpy
        from rclpy.serialization import serialize_message
        from sensor_msgs.msg import Image
        from std_msgs.msg import String
        rclpy.init()
        ros = rclpy.create_node(f"writer_e2e_{os.getpid()}")
        text = ros.create_publisher(String, "/record_test/text", 10)
        image = ros.create_publisher(Image, "/record_test/image", 10)
        bridge = Path(__file__).with_name("ros2_raw_bridge.py").resolve()
        for name, kind in (("text", "std_msgs/msg/String"), ("image", "sensor_msgs/msg/Image")):
            topic = f"/record_test/{name}"
            parts.append(f'[[sources]]\nid="ros-{name}"\nkind="ros2"\ntopic="{topic}"\n'
                         f'message_type="{kind}"\nqos="reliable"\nmax_payload_bytes=65536\nbridge={json.dumps(str(bridge))}\n')
            expected[topic] = []
    config.write_text("\n".join(parts))
    argv = [str(binary), "record-live", "--config", str(config), "--output", str(data),
            "--duration-sec", str(args.seconds)]
    start = time.monotonic()
    with (args.out / "record.log").open("w") as log, subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT) as process:
        try:
            limit = time.monotonic() + 19
            while not data.exists() or (ros and not (text.get_subscription_count() and image.get_subscription_count())):
                if process.poll() is not None or time.monotonic() > limit:
                    raise RuntimeError("recorder or ROS discovery did not start; inspect record.log")
                time.sleep(.01)
            time.sleep(.15)
            # Deliberately send one truncated datagram per UDP source before the valid stream.
            for sender, port in udp:
                sender.sendto(bytes(600), ("127.0.0.1", port))
            deadline = time.monotonic()
            for i in range((args.seconds - 2) * 100):
                payload = b"" if i == 0 else i.to_bytes(4, "little") + bytes([i % 251]) * (i % 167)
                for (sender, port), transform in zip(udp, ("identity", "zstd")):
                    sender.sendto(payload, ("127.0.0.1", port))
                    expected[f"/udp/udp-{transform}"].append(payload)
                if can:
                    body = i.to_bytes(4, "little")
                    can.send(struct.pack("=IB3x8s", 0x321, len(body), body))
                    expected["/socketcan/can"].append(body)
                    fd = body * 3
                    can.send(struct.pack("=IBB2x64s", 0x80001234, len(fd), 1, fd))
                    expected["/socketcan/can"].append(fd)
                if ros and i % 10 == 0:
                    a = String(data=f"payload-{i}")
                    b = Image(height=1, width=4, encoding="mono8", step=4, data=bytes([i % 256]) * 4)
                    text.publish(a)
                    image.publish(b)
                    expected["/record_test/text"].append(bytes(serialize_message(a)))
                    expected["/record_test/image"].append(bytes(serialize_message(b)))
                deadline += .01
                time.sleep(max(0, deadline - time.monotonic()))
            if args.stop_early:
                time.sleep(.1)
                if process.poll() is not None:
                    raise RuntimeError("recorder exited before SIGINT")
                process.send_signal(signal.SIGINT)
            if process.wait(timeout=args.seconds + 20):
                raise RuntimeError("recording failed; inspect record.log")
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
    for command in ("verify-live", "recover"):
        with (args.out / f"{command}.json").open("w") as log:
            subprocess.run([str(binary), command, "--input", str(data)], stdout=log, check=True)
    assert not json.loads((args.out / "recover.json").read_text())["recovery_needed"]
    actual = {key: [] for key in expected}
    for info, payload in messages(data):
        topic = info["topic"]
        index = len(actual[topic])
        if topic.startswith("/udp/"):
            sender, port = udp[topic.endswith("zstd")]
            assert info["source"] == info["destination"] == "127.0.0.1"
            assert info["destination_port"] == port and info["source_port"] == sender.getsockname()[1]
            assert info["interface_index"] == socket.if_nametoindex("lo")
            assert info["transform"] == topic.split("-")[-1]
            assert info["sequence"] == index + 1
        elif topic == "/socketcan/can":
            assert info["interface_index"] == socket.if_nametoindex(args.interface)
            if index % 2:
                assert info["can_id"] == 0x1234 and info["fd"] and info["brs"] and info["eff"]
            else:
                assert info["can_id"] == 0x321 and not info["fd"] and info["dlc"] == 4
        actual[topic].append(payload)
    assert actual == expected, {topic: (len(actual[topic]), len(values)) for topic, values in expected.items()}
    verified = json.loads((args.out / "verify-live.json").read_text())
    assert verified["known_loss"] == len(udp)
    report = json.loads((args.out / "run-summary.json").read_text())
    result = dict(mode=args.mode, writer=args.writer, sigint=args.stop_early, verified=verified, queue_peak=report["queue_peak"],
                  cpu_ns=report["cpu_ns"], peak_rss_bytes=report.get("peak_rss_bytes"),
                  mcap_bytes=data.stat().st_size, sources={k: len(v) for k, v in actual.items()},
                  wall_seconds=time.monotonic() - start)
    print(json.dumps(result))
    (args.out / "e2e.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
