# Durability performance

Step 2 compared the original Writer at `091f685` to the isolated Barrier implementation. The same two 1 MiB Mock V4L2 sources, 16 MiB / 16-message / 80 ms commit policy, CPU affinity, and ext4/NVMe were used in three rotated runs. Each run produced at most 180 MiB. These are buffered, bounded capture/drain tests.

| Input | Before GB/s | Barrier GB/s | Before peak queue (worst run) | Barrier peak queue (worst run) |
|---|---:|---:|---:|---:|
| Paced, nominal 1.05 GB/s | 1.025 | 1.016 | 13 | 13 |
| Short burst, nominal 2.10 GB/s | 1.612 | 1.512 | 46 | 54 |

All 12 runs passed the independent Verifier with zero loss, Unknown, corruption and deadline overruns. Recovery has an existing 128 MiB limit, so it was tested by the small Rust regression fixtures. The benchmark leaves recovery unset for larger files. Barrier waits for both data and journal sync. The old Writer could submit the next group while Monitor processed the journal. The extra wait is present in the measurements.

[Step 2 CSV](durability-evidence/baseline.csv) · [Step 2 JSON](durability-evidence/baseline.json)
