"""Sequential buffered write, with a hard byte limit and NVMe temperature checks."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

MIB = 1 << 20
MAX_MIB = 8192
GROUP = 64 * MIB
CHUNK = 4 * MIB


def device_for(path):
    dev = path.stat().st_dev
    block = Path(f"/sys/dev/block/{os.major(dev)}:{os.minor(dev)}").resolve(strict=True)
    controller = next((part for part in block.parents if part.parent.name == "nvme"), None)
    if controller is None:
        raise ValueError("output must map directly to an NVMe device")
    sensors = [p / "temp1_input" for p in Path("/sys/class/hwmon").glob("hwmon*")
               if (p / "device").resolve() == controller and (p / "temp1_input").exists()]
    if len(sensors) != 1:
        raise ValueError("could not uniquely resolve the output NVMe temperature")
    disk = block.parent if (block / "partition").exists() else block
    return disk, sensors[0], controller


def sample(disk, sensor):
    stat = [int(field) for field in (disk / "stat").read_text().split()]
    return dict(time_ns=time.monotonic_ns(), temperature_c=int(sensor.read_text()) / 1000,
                disk_written_bytes=stat[6] * 512, disk_io_busy_ms=stat[9])


def run(output, mib, seconds):
    disk, sensor, controller = device_for(output.parent)
    limit = mib * MIB
    free = shutil.disk_usage(output.parent).free
    if free < max(2 * limit, 4 * (1 << 30)):
        raise ValueError("insufficient space for bounded test")
    preflight = sample(disk, sensor)
    if not 0 < preflight["temperature_c"] < 65:
        raise ValueError("NVMe temperature outside test range")
    output.mkdir()
    data = output / "write.bin"
    payload = os.urandom(CHUNK)
    manifest = dict(disk=disk.name, sensor=str(sensor), free_bytes=free, byte_limit=limit,
                    time_limit_s=seconds, temperature_limit_c=65,
                    mount=subprocess.check_output(["findmnt", "-T", str(output)], text=True),
                    model=(controller / "model").read_text().strip(),
                    firmware=(controller / "firmware_rev").read_text().strip())
    print(json.dumps(manifest), flush=True)
    written = 0
    samples = []
    with data.open("xb", buffering=0) as stream:
        start = sample(disk, sensor)
        samples.append(dict(committed_bytes=0, **start))
        status = "byte_limit"
        while written < limit:
            size = min(GROUP, limit - written)
            for _ in range(size // CHUNK):
                if stream.write(payload) != CHUNK:
                    raise IOError("short write")
            written += size
            sync_start = time.monotonic_ns()
            os.fdatasync(stream.fileno())
            row = dict(committed_bytes=written, sync_ns=time.monotonic_ns() - sync_start,
                       **sample(disk, sensor))
            samples.append(row)
            if not 0 < row["temperature_c"] < 65:
                status = "temperature_limit"
                break
            if row["time_ns"] - start["time_ns"] >= seconds * 10**9:
                status = "time_limit"
                break
    with data.open("rb", buffering=0) as stream:
        for offset in (0, (written // CHUNK // 2) * CHUNK, written - CHUNK):
            stream.seek(offset)
            if stream.read(CHUNK) != payload:
                raise IOError("sampled verification failed")
    if data.stat().st_size != written:
        raise IOError("length check failed")
    aggregate = dict(manifest=manifest, status=status, samples=samples, verification="length+3 samples")
    with (output / "result.json").open("x") as stream:
        json.dump(aggregate, stream, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--mib", type=int, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    args = parser.parse_args()
    if not 64 <= args.mib <= MAX_MIB or args.mib % 64 or not 1 <= args.seconds <= 30:
        parser.error("use a 64 MiB multiple up to 8192 MiB and 1–30 seconds")
    run(args.out.resolve(), args.mib, args.seconds)


if __name__ == "__main__":
    main()
