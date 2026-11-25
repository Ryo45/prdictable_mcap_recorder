use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_recorder"))
}

fn execute(output: Output, success: bool) -> Value {
    assert_eq!(
        output.status.success(),
        success,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn verify(directory: &Path, success: bool) -> Value {
    execute(
        cli()
            .current_dir(directory)
            .args([
                "verify",
                "--input",
                "run.mcap",
                "--report",
                "run-summary.json",
            ])
            .output()
            .unwrap(),
        success,
    )
}

const CONFIG: &str = r#"
[record]
memory_budget_bytes = 33554432
queue_capacity = 16
commit_messages = 2
commit_interval_ms = 500

[storage_profile]
sustained_bytes_per_sec = 1000000
max_stall_ms = 20
max_io_us = 10000
max_sync_ms = 50

[copy_profile]
assumed_min_bytes_per_sec = 10000000
return_margin_us = 800
scheduling_margin_us = 2000
max_capture_delay_us = 40000

[[sources]]
id = "a"
model = "fixed-deadline"
frame_bytes = 128
fps = 5
max_hold_ms = 80
dma_buffers = 6
capture_reserve = 1
return_delay_us = 400

[[sources]]
id = "b"
model = "pool"
frame_bytes = 256
fps = 5
burst = 2
dma_buffers = 8
capture_reserve = 1
phase_us = 10000

[[spill]]
frame_bytes = 128
buffers = 5

[[spill]]
frame_bytes = 256
buffers = 8
"#;

fn capture(path: &Path, config: &str, success: bool, flags: &[&str]) -> Value {
    fs::write(path.join("run.toml"), config).unwrap();
    let output = cli()
        .current_dir(path)
        .args(["record", "--config", "run.toml", "--duration-sec", "0.48"])
        .args(flags)
        .output()
        .unwrap();
    assert_eq!(
        output.status.success(),
        success,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&fs::read(path.join("run-summary.json")).unwrap()).unwrap()
}

#[test]
fn multi_source_round_trip_checks_the_actual_commit_membership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let summary = capture(path, CONFIG, true, &[]);
    assert!(summary["all_resources_returned"].as_bool().unwrap());
    let result = verify(path, true);
    assert_eq!(result["committed_and_verified"], 9);
    assert!(result["commit_groups_read"].as_u64().unwrap() >= 4);

    let data = fs::read(path.join("run.mcap")).unwrap();
    let actual: Vec<_> = mcap::MessageStream::new(&data)
        .unwrap()
        .collect::<mcap::McapResult<_>>()
        .unwrap();
    assert_eq!(actual.len(), 9);
    assert!(
        actual
            .iter()
            .any(|message| message.channel.topic == "/synthetic/a")
    );
    assert!(
        actual
            .iter()
            .any(|message| message.channel.topic == "/synthetic/b")
    );

    let commit_path = path.join("run-commits.jsonl");
    let ledger = fs::read_to_string(&commit_path).unwrap();
    let mut entries: Vec<Value> = ledger
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let span = entries
        .iter_mut()
        .find(|entry| entry["type"] == "frames" && entry["stream"] == 0)
        .unwrap();
    span["start"] = Value::from(1);
    span["end"] = Value::from(2);
    let changed: String = entries.iter().map(|entry| format!("{entry}\n")).collect();
    fs::write(&commit_path, changed).unwrap();
    let changed = verify(path, false);
    assert!(
        changed["errors"]
            .to_string()
            .contains("absent from its committed group")
    );

    fs::write(commit_path, ledger).unwrap();
    let mut data = data;
    let mut offset = mcap::MAGIC.len();
    while data[offset] != mcap::records::op::MESSAGE {
        let length = u64::from_le_bytes(data[offset + 1..offset + 9].try_into().unwrap());
        offset += 9 + length as usize;
    }
    data[offset + 31 + 14] ^= 0x80;
    fs::write(path.join("run.mcap"), data).unwrap();
    let corrupt = verify(path, false);
    assert_eq!(corrupt["corrupt_frames"], 1);
}

#[test]
fn failed_sync_does_not_convert_written_frames_to_committed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let cfg = format!("{CONFIG}\n[faults]\nsync_fail_group = 1\n");
    let report = capture(path, &cfg, false, &[]);
    assert_eq!(report["group_count"], 1);
    assert_eq!(report["final_sync"], false);
    assert_eq!(report["status"], "error");
    assert_eq!(report["all_resources_returned"], true);
    let result = verify(path, false);
    assert!(result["unknown"].as_u64().unwrap() > 0);
    assert_eq!(result["committed_and_verified"], 2);
    assert_eq!(result["unreported_missing"], 0);
    assert_eq!(result["false_loss_claims"], 0);
}

#[test]
fn unknown_contract_needs_explicit_run_permission() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let config = CONFIG.replace("max_io_us = 10000", "");
    fs::write(path.join("run.toml"), &config).unwrap();
    let plan = execute(
        cli()
            .current_dir(path)
            .args(["plan", "--config", "run.toml"])
            .output()
            .unwrap(),
        true,
    );
    assert_eq!(plan["verdict"], "unknown");
    let denied = cli()
        .current_dir(path)
        .args(["record", "--config", "run.toml"])
        .output()
        .unwrap();
    assert!(!denied.status.success());
    assert!(!path.join("run.mcap").exists());
    let report = capture(path, &config, true, &["--allow-unverified"]);
    assert_eq!(report["within_assumed_contract"], false);
    assert_eq!(verify(path, true)["committed_and_verified"], 9);
}

#[test]
fn stop_waits_for_mock_io_completion_and_classifies_the_uncaptured_tail() {
    use std::thread;
    use std::time::{Duration, Instant};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    let cfg = CONFIG.replace(
        "[record]",
        "[record]\nwriter = \"mock-async\"\nin_flight = 3\nmetrics_interval_ms = 10",
    );
    fs::write(
        path.join("run.toml"),
        format!("{cfg}\n[faults]\nmock_delay_us = 1000000\n"),
    )
    .unwrap();
    let mut process = cli()
        .current_dir(path)
        .args(["record", "--config", "run.toml", "--duration-sec", "8"])
        .spawn()
        .unwrap();

    let mut observed = false;
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline {
        let metrics = fs::read_to_string(path.join("run-metrics.csv")).unwrap_or_default();
        let mut rows = metrics.lines();
        if let Some(index) = rows
            .next()
            .and_then(|header| header.split(',').position(|field| field == "in_flight"))
            && rows.any(|row| {
                row.split(',')
                    .nth(index)
                    .and_then(|field| field.parse::<u64>().ok())
                    .is_some_and(|n| n > 0)
            })
        {
            observed = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    // An in-flight sample also establishes that the child's stop handler is active.
    let signal = unsafe { libc::kill(process.id() as libc::pid_t, libc::SIGTERM) };
    let exit = process.wait().unwrap();
    assert!(observed);
    assert_eq!(signal, 0);
    assert_eq!(exit.code(), Some(1));

    let report: Value =
        serde_json::from_slice(&fs::read(path.join("run-summary.json")).unwrap()).unwrap();
    assert_eq!(report["status"], "cancelled");
    assert_eq!(report["all_resources_returned"], true);
    assert_eq!(report["final_sync"], true);
    let result = verify(path, false);
    assert!(result["unknown"].as_u64().unwrap() > 0);
    assert!(result["committed_and_verified"].as_u64().unwrap() > 0);
    assert_eq!(result["unreported_missing"], 0);
}
