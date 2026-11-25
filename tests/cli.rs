use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_recorder"))
}

fn success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn summary(directory: &Path) -> Value {
    serde_json::from_slice(&fs::read(directory.join("run-summary.json")).unwrap()).unwrap()
}

fn verify(directory: &Path) -> Output {
    cli()
        .current_dir(directory)
        .args([
            "verify",
            "--input",
            "run.mcap",
            "--report",
            "run-summary.json",
            "--loss-log",
            "run-loss.jsonl",
        ])
        .output()
        .unwrap()
}

#[test]
fn all_three_policies_record_sync_drain_and_verify_with_the_official_reader() {
    for policy in ["copy", "borrow", "adaptive"] {
        let directory = tempfile::tempdir().unwrap();
        success(
            cli()
                .current_dir(directory.path())
                .args([
                    "record",
                    "--policy",
                    policy,
                    "--fps",
                    "5",
                    "--duration-sec",
                    "0.5",
                    "--frame-bytes",
                    "4096",
                ])
                .output()
                .unwrap(),
        );
        let report = summary(directory.path());
        assert_eq!(report["status"], "ok");
        assert_eq!(report["expected"], 3);
        assert_eq!(report["written"], 3);
        assert_eq!(report["dma"]["free"], report["dma"]["total"]);
        assert_eq!(report["spill"]["free"], report["spill"]["total"]);
        assert_eq!(report["queue_depth"], 0);
        assert_eq!(report["copy_count"], if policy == "copy" { 3 } else { 0 });
        let result: Value =
            serde_json::from_slice(&success(verify(directory.path())).stdout).unwrap();
        assert_eq!(result["verified"], 3);

        let bytes = fs::read(directory.path().join("run.mcap")).unwrap();
        let messages: Vec<_> = mcap::MessageStream::new(&bytes)
            .unwrap()
            .collect::<mcap::McapResult<_>>()
            .unwrap();
        assert_eq!(messages.len(), 3);
        for (sequence, message) in messages.iter().enumerate() {
            assert_eq!(message.sequence, sequence as u32);
            assert_eq!(message.data.len(), 4096);
        }

        assert!(
            !cli()
                .current_dir(directory.path())
                .arg("record")
                .output()
                .unwrap()
                .status
                .success()
        );
        assert_eq!(fs::read(directory.path().join("run.mcap")).unwrap(), bytes);
    }
}

#[test]
fn signal_stop_flushes_the_queue_and_reports_the_unattempted_tail() {
    let directory = tempfile::tempdir().unwrap();
    let mut child = cli()
        .current_dir(directory.path())
        .args([
            "record",
            "--fps",
            "20",
            "--duration-sec",
            "10",
            "--frame-bytes",
            "1024",
            "--writer-bytes-per-sec",
            "1024",
            "--metrics-interval-ms",
            "10",
        ])
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let metrics = directory.path().join("run-metrics.csv");
    let observed = loop {
        if fs::read_to_string(&metrics)
            .unwrap_or_default()
            .lines()
            .skip(1)
            .any(|row| {
                row.split(',')
                    .nth(2)
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_some_and(|received| received > 0)
            })
        {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        thread::sleep(Duration::from_millis(5));
    };
    // The child has installed its handlers before it records the sampled frame.
    let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    let exit = child.wait().unwrap();
    assert!(observed);
    assert_eq!(result, 0);
    assert_eq!(exit.code(), Some(1));
    let report = summary(directory.path());
    assert_eq!(report["status"], "cancelled");
    assert_eq!(report["mcap_finalized"], true);
    assert_eq!(report["mcap_synced"], true);
    assert!(report["losses"]["unknown"].as_u64().unwrap() > 0);
    assert_eq!(report["written"], report["received"]);
    assert_eq!(report["dma"]["free"], report["dma"]["total"]);
    assert_eq!(report["spill"]["free"], report["spill"]["total"]);
    assert_eq!(report["queue_depth"], 0);
    let verification = verify(directory.path());
    assert!(!verification.status.success());
    let result: Value = serde_json::from_slice(&verification.stdout).unwrap();
    assert_eq!(result["finalized"], true);
    assert_eq!(result["unreported_missing"], 0);
}

#[test]
fn invalid_and_oversized_configurations_fail_before_creating_outputs() {
    let directory = tempfile::tempdir().unwrap();
    for options in [
        ["--frame-bytes", "9"],
        ["--capture-reserve", "8"],
        ["--queue-capacity", "0"],
        ["--loss-queue-capacity", "18446744073709551615"],
        ["--duration-sec", "0"],
        ["--fps", "0"],
        ["--writer-bytes-per-sec", "0"],
        ["--duration-sec", "1000"],
    ] {
        assert!(
            !cli()
                .current_dir(directory.path())
                .arg("record")
                .args(options)
                .output()
                .unwrap()
                .status
                .success()
        );
    }
    assert!(fs::read_dir(directory.path()).unwrap().next().is_none());
}

#[test]
fn queue_overload_is_a_verified_acquisition_loss() {
    let directory = tempfile::tempdir().unwrap();
    success(
        cli()
            .current_dir(directory.path())
            .args([
                "record",
                "--fps",
                "10",
                "--duration-sec",
                "0.4",
                "--frame-bytes",
                "1024",
                "--queue-capacity",
                "1",
                "--stall-at-ms",
                "0",
                "--stall-duration-ms",
                "500",
            ])
            .output()
            .unwrap(),
    );
    let report = summary(directory.path());
    assert_eq!(report["expected"], 4);
    assert_eq!(report["received"], 1);
    assert_eq!(report["written"], 1);
    assert_eq!(report["losses"]["queue_full"], 3);
    assert_eq!(report["queue_peak"], 1);
    let checked: Value = serde_json::from_slice(&success(verify(directory.path())).stdout).unwrap();
    assert_eq!(checked["journaled_losses"], 3);
    assert_eq!(checked["unreported_missing"], 0);
}
