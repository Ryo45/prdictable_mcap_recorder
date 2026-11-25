# Durability performance

Step 2 compared the original Writer at `091f685` to the isolated Barrier implementation. The same two 1 MiB Mock V4L2 sources, 16 MiB / 16-message / 80 ms commit policy, CPU affinity, and ext4/NVMe were used in three rotated runs. Each run produced at most 180 MiB. These are buffered, bounded capture/drain tests.

| Input | Before GB/s | Barrier GB/s | Before peak queue (worst run) | Barrier peak queue (worst run) |
|---|---:|---:|---:|---:|
| Paced, nominal 1.05 GB/s | 1.025 | 1.016 | 13 | 13 |
| Short burst, nominal 2.10 GB/s | 1.612 | 1.512 | 46 | 54 |

All 12 runs passed the independent Verifier with zero loss, Unknown, corruption and deadline overruns. Recovery has an existing 128 MiB limit, so it was tested by the small Rust regression fixtures. The benchmark leaves recovery unset for larger files. Barrier waits for both data and journal sync. The old Writer could submit the next group while Monitor processed the journal. The extra wait is present in the measurements.

[Step 2 CSV](durability-evidence/baseline.csv) · [Step 2 JSON](durability-evidence/baseline.json)

## Step 4 — Overlap and sync tuning

Thirty runs used 1 MiB Mock V4L2 frames, two sources, three rotated repetitions per cell, CPU 0–7 and the same KIOXIA NVMe/ext4 volume. The paced test runs at nominal 1.05 GB/s for 0.18 s. The short burst runs at nominal 2.10 GB/s for 0.09 s and drains afterward. Each trial writes at most about 180 MiB. The batch cap was 6 GiB, with 99.9 GB free at start. The recorder subprocess deadline was 35 s; the script checked NVMe temperature before and after each run against 65°C. Observed temperatures were 24.85–26.85°C.

| Input | Policy | Recorded GB/s median | Process CPU s/GiB | Writer CPU s/GiB | Queue max | Loss total |
|---|---|---:|---:|---:|---:|---:|
| Paced | Before | 1.025 | 0.685 | — | 12 | 0 |
| Paced | Barrier 16 MiB | 1.015 | 0.710 | 0.240 | 12 | 0 |
| Paced | Overlap 16 MiB | 1.020 | 0.687 | 0.219 | 2 | 0 |
| Paced | Overlap 4 MiB | 1.029 | 0.704 | 0.226 | 3 | 0 |
| Paced | Overlap 30 ms | 0.992 | 0.705 | 0.219 | 3 | 0 |
| Burst | Before | 1.573 | 0.733 | — | 61 | 0 |
| Burst | Barrier 16 MiB | 1.433 | 0.737 | 0.275 | 64 | 3 |
| Burst | Overlap 16 MiB | 1.929 | 0.703 | 0.212 | 5 | 0 |
| Burst | Overlap 4 MiB | 1.282 | 0.775 | 0.247 | 64 | 11 |
| Burst | Overlap 30 ms | 1.708 | 0.693 | 0.209 | 7 | 0 |

All 30 runs passed the independent Verifier and the original binary also verified the 24 new recordings. Reported loss was included in the verification; it does not count as a gap in the evidence. Unknown, corruption and deadline overruns were zero throughout. CopyOut was zero and in-flight peak was one in these synchronous performance trials. The forced-stall Mock Async regression covered in-flight reordering separately.

Before reports from session start through its final data-sync path. Barrier and Overlap report through Journal confirmation, including final drain. The separate data-sync endpoint in the CSV yields paced medians 1.025 / 1.019 / 1.024 and burst medians 1.573 / 1.442 / 1.945 GB/s for Before / Barrier / 16 MiB Overlap. Device counter deltas cover the full NVMe, including concurrent activity. No internal cache exhaustion or steady-state saturation was established.

At high pace, Barrier spent median **87.5 ms** waiting in the combined data and journal sync path. The 16 MiB Overlap spent **1.37 ms** capacity-gated, and at least **22.2 ms** of measured submit calls were entirely inside a data-sync interval. The median per-run peak unconfirmed message bytes was about **43 MiB**, compared with **16 MiB** for Barrier. The high-pace maximum among per-source acquire-to-reuse p99 values was **39.1 / 42.5 / 3.0 ms** for Before / Barrier / 16 MiB Overlap. The logical delay from Lease Drop to Mock buffer eligibility is separately configured at 200 µs. The experiments do not measure a hardware DMA ACK.

The 4 MiB policy reduced the unconfirmed window to about 12 MiB but spent median **93.9 ms** awaiting capacity at high pace, leading to loss. The 30 ms policy reached about 111 MiB unconfirmed at high pace. Neither is the recommended overlap setting. Keep **Barrier as the default**, with the existing commit threshold defaults. For the tested high-paced envelope, the proven opt-in candidate is **Overlap with 16 MiB / 16 messages / 80 ms**, two sealed groups and a 128 MiB cap. The Barrier high-pace regression requires qualification against the actual capture envelope. Do not infer the default is lossless for that workload. Physical io_uring, actual device leases and power-failure behavior remain unqualified.

## Reproduce

Build the original binary from `091f685` and the candidate from the durability branch. Use a new directory on the target NVMe, with at least 12 GiB free:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 tools/bench_durability.py \
  --before /path/to/original/recorder --recorder target/release/recorder \
  --out /path/on/nvme/new-durability-run --tune
```

Without `--tune`, the runner only compares Before and Barrier. JSON includes the command, exact configuration, CPU affinity, binary SHA-256, filesystem and temperature. Recovery is run for files within its current size limit; larger trial files use the independent Verifier. Small-fixture recovery and SIGKILL are covered by Rust tests. The final recovery guard for a reported journal rollback failure runs off the recording path.

[Step 4 CSV](durability-evidence/tuning.csv) · [Step 4 JSON](durability-evidence/tuning.json)
