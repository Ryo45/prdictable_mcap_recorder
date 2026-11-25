use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

fn setup(mode: &str, writer: &str, faults: &str) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let config = format!(
        r#"
[record]
writer = "{writer}"
policy = "always-borrow"
queue_capacity = 80
in_flight = 4
max_borrowed_in_flight = 4
memory_budget_bytes = 67108864
commit_messages = 99

[durability]
mode = "{mode}"
commit_messages = 4
max_pending_groups = 2
event_queue_capacity = 1
max_pending_bytes = 66000

[faults]
{faults}

[[sources]]
kind = "mock_v4l2"
id = "camera"
model = "fixed-deadline"
frame_bytes = 8192
fps = 150
burst = 2
dma_buffers = 80
max_hold_ms = 250
return_delay_us = 300
"#
    );
    fs::write(dir.path().join("config.toml"), config).unwrap();
    dir
}

fn record(path: &Path, seconds: &str) -> Command {
    let mut runner = command(path);
    runner.args([
        "record",
        "--config",
        "config.toml",
        "--output",
        "run.mcap",
        "--duration-sec",
        seconds,
        "--allow-unverified",
    ]);
    runner
}

fn fixture(mode: &str, writer: &str, faults: &str) -> (TempDir, Output) {
    let dir = setup(mode, writer, faults);
    let output = record(dir.path(), "0.16").output().unwrap();
    (dir, output)
}

fn command(path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_recorder"));
    command.current_dir(path);
    command
}

fn read(path: &Path, file: &str) -> Value {
    serde_json::from_slice(&fs::read(path.join(file)).unwrap()).unwrap()
}

fn recover(path: &Path) -> Value {
    let output = command(path)
        .args(["recover", "--input", "run.mcap", "--dry-run"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn verify(path: &Path) {
    let output = command(path)
        .args([
            "verify",
            "--input",
            "run.mcap",
            "--report",
            "run-summary.json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn overlap_drains_io_during_sync_with_a_bounded_mailbox() {
    let (dir, output) = fixture(
        "overlap",
        "mock-async",
        "mock_delay_us = 2000\ndurability_sync_delay_ms = 20\nio_short_bytes = 4093",
    );
    assert!(output.status.success(), "{output:?}");
    let report = read(dir.path(), "run-summary.json");
    let dur = &report["durability"];
    assert_eq!(dur["pending_peak"], 1);
    assert!(dur["backpressure_ns"].as_u64().unwrap() > 0);
    assert!(dur["max_uncertain_bytes"].as_u64().unwrap() <= 66000);
    assert!(dur["completions_during_sync"].as_u64().unwrap() > 0);
    assert!(
        report["writer"]["peak_completion_reorder"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(report["devices"][0]["pool"]["deadline_exceeded"], 0);
    assert_eq!(report["all_resources_returned"], true);
    assert_eq!(report["settings"]["record"]["commit_messages"], 4);
    assert_eq!(report["commit_log_synced"], true);
    assert_eq!(recover(dir.path())["recovery_needed"], false);
    verify(dir.path());
}

#[test]
fn journal_failure_retains_only_the_preceding_group() {
    for mode in ["barrier", "overlap"] {
        let (dir, output) = fixture(mode, "sync", "journal_fail_group = 1");
        assert!(!output.status.success(), "{output:?}");
        let report = read(dir.path(), "run-summary.json");
        assert_eq!(report["durability"]["data_syncs"], 2);
        assert_eq!(report["durability"]["journal"]["failures"], 1);
        assert_eq!(report["commit_log_synced"], false);
        assert_eq!(report["sources"][0]["committed"], 4);
        assert!(report["sources"][0]["unknown"].as_u64().unwrap() >= 4);
        assert_eq!(report["all_resources_returned"], true);
        let recovered = recover(dir.path());
        assert_eq!(recovered["committed_with_evidence"], 4);
        assert!(recovered["recovered_but_unconfirmed"].as_u64().unwrap() >= 4);
    }
}

#[test]
fn worker_stop_and_sync_failure_do_not_certify_following_groups() {
    for fault in ["durability_stop_group = 1", "sync_fail_group = 1"] {
        let (dir, output) = fixture("overlap", "sync", fault);
        assert!(!output.status.success(), "{output:?}");
        let report = read(dir.path(), "run-summary.json");
        assert_eq!(report["sources"][0]["committed"], 4);
        assert!(report["sources"][0]["unknown"].as_u64().unwrap() >= 4);
        assert_eq!(report["all_resources_returned"], true);
        assert_eq!(recover(dir.path())["committed_with_evidence"], 4);
    }
}

#[test]
fn failed_message_cannot_be_committed_on_eio_or_enospc() {
    for kind in ["eio", "enospc"] {
        let faults = format!("io_error_request = 5\nio_error = \"{kind}\"");
        let (dir, output) = fixture("overlap", "mock-async", &faults);
        assert!(!output.status.success(), "{output:?}");
        let report = read(dir.path(), "run-summary.json");
        assert_eq!(report["all_resources_returned"], true);
        assert_eq!(report["sources"][0]["committed"], 4);
        assert_eq!(recover(dir.path())["committed_with_evidence"], 4);
    }
}

#[test]
fn overlap_kill_recovers_only_the_evidenced_prefix() {
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    let dir = setup("overlap", "sync", "durability_sync_delay_ms = 80");
    let mut child = record(dir.path(), "4")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stop = Instant::now() + Duration::from_secs(3);
    let path = dir.path();
    let mut observed = false;
    while Instant::now() < stop {
        let journal = fs::read_to_string(path.join("run-commits.jsonl")).unwrap_or_default();
        if journal.contains(r#""type":"frames""#)
            && fs::metadata(path.join("run.mcap")).is_ok_and(|data| data.len() > 6 * 8192)
        {
            observed = true;
            break;
        }
        if child.try_wait().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let _ = child.kill();
    child.wait().unwrap();
    assert!(observed);
    let result = recover(path);
    assert!(result["committed_with_evidence"].as_u64().unwrap() >= 4);
    assert!(result["recovered_but_unconfirmed"].as_u64().unwrap() >= 1);
}

#[test]
fn overlap_rejects_zero_or_impossible_limits_before_recording() {
    for (valid, invalid) in [
        ("event_queue_capacity = 1", "event_queue_capacity = 0"),
        ("max_pending_groups = 2", "max_pending_groups = 0"),
        ("max_pending_bytes = 66000", "max_pending_bytes = 8192"),
    ] {
        let dir = setup("overlap", "sync", "");
        let path = dir.path().join("config.toml");
        let cfg = fs::read_to_string(&path).unwrap().replace(valid, invalid);
        fs::write(path, cfg).unwrap();
        assert!(
            !record(dir.path(), "0.16")
                .output()
                .unwrap()
                .status
                .success()
        );
        assert!(!dir.path().join("run.mcap").exists());
    }
}

#[test]
fn recovery_does_not_trust_a_readable_journal_after_reported_rollback_failure() {
    let (dir, output) = fixture("overlap", "sync", "");
    assert!(output.status.success(), "{output:?}");
    let mut report = read(dir.path(), "run-summary.json");
    report["durability"]["journal"]["rollback_failed"] = true.into();
    fs::write(
        dir.path().join("run-summary.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let recovered = recover(dir.path());
    assert_eq!(recovered["committed_with_evidence"], 0);
    assert_eq!(
        recovered["recovered_but_unconfirmed"],
        recovered["readable_messages"]
    );
}
