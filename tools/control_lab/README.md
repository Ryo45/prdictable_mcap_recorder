# Recorder control lab

Python 3.11 or newer. With the repository root as the working directory:

```sh
python3 -m unittest discover -s tools/control_lab/tests -v
```

See [the model](../../docs/control-model.md) for the scenario and trace fields, ownership transitions and baseline differences. [Phase 0](../../docs/writer-capacity-baseline.md) documents the separate I/O benchmark and its output cap.

## Phase 2 comparison

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m tools.control_lab.compare --out target/control-trial
```

The output directory must be new. The command writes paired CSV/JSON, the exact scenario inputs, training choices and selected event traces. It generates no MCAP or SSD load. Read the [Phase 2 report](../../docs/control-oracle-results.md) for the safety certificates, Oracle decision domain, timing caveats and adoption decision.
