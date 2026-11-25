use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::tempdir;

fn recorder() -> Command {
    Command::new(env!("CARGO_BIN_EXE_recorder"))
}

fn run(command: &mut Command) -> Value {
    let result = command.output().unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}

fn demo(directory: &Path) {
    let result = recorder()
        .args([
            "record",
            "--config",
            "configs/v3-smoke.toml",
            "--duration-sec",
            "0.6",
            "--output",
        ])
        .arg(directory.join("run.mcap"))
        .output()
        .unwrap();
    assert!(result.status.success(), "{:?}", result);
}

#[test]
fn all_mock_contracts_round_trip() {
    let dir = tempdir().unwrap();
    demo(dir.path());
    let data = dir.path().join("run.mcap");
    let report = run(recorder()
        .args(["verify", "--input"])
        .arg(&data)
        .arg("--report")
        .arg(dir.path().join("run-summary.json")));
    assert_eq!(report["verified"], 190);
    assert_eq!(report["unreported_missing"], 0);
    assert_eq!(report["sources"][3]["before_accept_loss"], 2);

    let bytes = fs::read(&data).unwrap();
    let read = mcap::MessageStream::new(&bytes).unwrap();
    let mut topics = std::collections::HashSet::new();
    for msg in read {
        let msg = msg.unwrap();
        topics.insert(msg.channel.topic.clone());
        if msg.channel.topic.starts_with("/ros_") {
            assert_eq!(msg.channel.message_encoding, "cdr");
            assert_eq!(&msg.data[..4], &[0, 1, 0, 0]);
            assert_eq!(
                u32::from_le_bytes(msg.data[8..12].try_into().unwrap()),
                msg.sequence
            );
            let schema = msg.channel.schema.as_ref().unwrap();
            assert_eq!(schema.encoding, "ros2msg");
            assert!(
                schema
                    .name
                    .ends_with(if msg.channel.topic == "/ros_scalar" {
                        "MockScalar"
                    } else {
                        "MockFrame"
                    })
            );
        }
        assert!(msg.log_time >= msg.publish_time);
    }
    assert_eq!(topics.len(), 8);

    let recovered = run(recorder()
        .args(["recover", "--dry-run", "--input"])
        .arg(&data));
    assert_eq!(recovered["recovery_needed"], false);
    assert_eq!(recovered["committed_with_evidence"], 190);
    assert_eq!(fs::read(data).unwrap(), bytes);
}

#[test]
fn torn_tail_and_journal_preserve_distinction_and_rebuild_indexes() {
    let dir = tempdir().unwrap();
    demo(dir.path());
    let data = dir.path().join("run.mcap");
    let output = dir.path().join("repaired.mcap");
    let mut bytes = fs::read(&data).unwrap();
    bytes.truncate(bytes.len() - 7);
    fs::write(&data, &bytes).unwrap();

    let journal = dir.path().join("run-commits.jsonl");
    let mut ledger = fs::read_to_string(&journal).unwrap();
    let boundary = ledger.rfind("\"type\":\"group\"").unwrap();
    let start = ledger[..boundary].rfind('\n').unwrap() + 1;
    ledger.truncate(start + 9);
    fs::write(&journal, &ledger).unwrap();

    let preview = run(recorder().args(["recover", "--input"]).arg(&data));
    assert_eq!(preview["finalized"], false);
    assert_eq!(preview["readable_messages"], 190);
    assert!(preview["committed_with_evidence"].as_u64().unwrap() > 0);
    assert!(preview["recovered_but_unconfirmed"].as_u64().unwrap() > 0);
    assert!(preview["journal_error"].is_string());
    assert!(!output.exists());

    let applied = run(recorder()
        .args(["recover", "--apply", "--input"])
        .arg(&data)
        .arg("--output")
        .arg(&output));
    assert_eq!(applied["readable_messages"], 190);
    assert_eq!(fs::read(&data).unwrap(), bytes);
    assert_eq!(fs::read_to_string(&journal).unwrap(), ledger);
    let repaired = fs::read(output).unwrap();
    let summary = mcap::Summary::read(&repaired).unwrap().unwrap();
    assert!(!summary.chunk_indexes.is_empty());
    assert!(
        summary.chunk_indexes.iter().all(|chunk| {
            chunk.compression.is_empty() && !chunk.message_index_offsets.is_empty()
        })
    );
    let mut indexed_count = 0;
    for chunk in &summary.chunk_indexes {
        for (channel, entries) in summary.read_message_indexes(&repaired, chunk).unwrap() {
            for entry in entries {
                let message = summary.seek_message(&repaired, chunk, &entry).unwrap();
                assert_eq!(message.channel.topic, channel.topic);
                assert_eq!(message.log_time, entry.log_time);
                assert!(
                    (chunk.message_start_time..=chunk.message_end_time).contains(&message.log_time)
                );
                indexed_count += 1;
            }
        }
    }
    assert_eq!(indexed_count, 190);
    assert_eq!(
        mcap::MessageStream::new(&repaired)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        190
    );
}

#[test]
fn process_kill_retains_only_evidenced_groups() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("run.mcap");
    let journal = dir.path().join("run-commits.jsonl");
    let mut child = recorder()
        .args([
            "record",
            "--config",
            "configs/v3-smoke.toml",
            "--duration-sec",
            "5",
            "--output",
        ])
        .arg(&data)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    let mut checkpoint = false;
    while start.elapsed() < Duration::from_secs(3) {
        if fs::read_to_string(&journal).is_ok_and(|text| text.contains("\"group_id\":1")) {
            checkpoint = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let kill = child.kill();
    let _ = child.wait();
    kill.unwrap();
    assert!(checkpoint, "no checkpoints observed");
    let recovered = run(recorder().args(["recover", "--input"]).arg(&data));
    assert_eq!(recovered["finalized"], false);
    assert!(recovered["committed_with_evidence"].as_u64().unwrap() > 0);
}

#[test]
fn failed_sync_leaves_readable_messages_unconfirmed() {
    let dir = tempdir().unwrap();
    let data = dir.path().join("run.mcap");
    let config = dir.path().join("fault.toml");
    fs::write(
        &config,
        format!(
            "{}\n[faults]\nsync_fail_group = 1\n",
            include_str!("../configs/v3-smoke.toml")
        ),
    )
    .unwrap();
    let result = recorder()
        .args(["record", "--duration-sec", "0.6", "--config"])
        .arg(config)
        .arg("--output")
        .arg(&data)
        .output()
        .unwrap();
    assert!(!result.status.success());
    let recovered = run(recorder().args(["recover", "--input"]).arg(&data));
    assert_eq!(recovered["finalized"], false);
    assert!(recovered["committed_with_evidence"].as_u64().unwrap() > 0);
    assert!(recovered["recovered_but_unconfirmed"].as_u64().unwrap() > 0);
}

#[test]
fn changing_commit_crc_invalidates_the_group_in_verifier_and_recovery() {
    let dir = tempdir().unwrap();
    demo(dir.path());
    let data = dir.path().join("run.mcap");
    let journal = dir.path().join("run-commits.jsonl");
    let ledger = fs::read_to_string(&journal).unwrap();
    let (header, remainder) = ledger.split_once('\n').unwrap();
    let mut header: Value = serde_json::from_str(header).unwrap();
    header["prefix_crc32"] = (header["prefix_crc32"].as_u64().unwrap() ^ 1).into();
    fs::write(journal, format!("{header}\n{remainder}")).unwrap();
    let verification = recorder()
        .args(["verify", "--input"])
        .arg(&data)
        .arg("--report")
        .arg(dir.path().join("run-summary.json"))
        .output()
        .unwrap();
    assert!(!verification.status.success());
    assert!(String::from_utf8_lossy(&verification.stdout).contains("commit prefix CRC mismatch"));
    let recovered = run(recorder().args(["recover", "--input"]).arg(&data));
    assert_eq!(recovered["finalized"], true);
    assert_eq!(recovered["committed_with_evidence"], 0);
    assert_eq!(recovered["recovered_but_unconfirmed"], 190);
}

#[test]
fn run_directory_publishes_only_successful_recordings() {
    let dir = tempdir().unwrap();
    let run_dir = dir.path().join("complete");
    let result = recorder()
        .args([
            "record",
            "--config",
            "configs/v3-smoke.toml",
            "--duration-sec",
            "0.1",
            "--output-dir",
        ])
        .arg(&run_dir)
        .output()
        .unwrap();
    assert!(result.status.success(), "{:?}", result);
    assert!(!run_dir.join("run.partial").exists());
    assert_eq!(
        run(recorder().args(["verify", "--run-dir"]).arg(&run_dir))["ok"],
        true
    );
    assert_eq!(
        run(recorder().args(["recover", "--run-dir"]).arg(&run_dir))["recovery_needed"],
        false
    );

    let config = dir.path().join("fault.toml");
    fs::write(
        &config,
        format!(
            "{}\n[faults]\nsync_fail_group = 0\n",
            include_str!("../configs/v3-smoke.toml")
        ),
    )
    .unwrap();
    let failed_dir = dir.path().join("failed");
    let result = recorder()
        .args(["record", "--config"])
        .arg(config)
        .arg("--output-dir")
        .arg(&failed_dir)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!failed_dir.join("run.mcap").exists());
    assert!(failed_dir.join("run.partial").exists());
    assert_eq!(
        run(recorder().args(["recover", "--run-dir"]).arg(&failed_dir))["committed_with_evidence"],
        0
    );
}

#[test]
fn recovery_reports_duplicate_kinds_and_leaves_repair_unpublished() {
    let dir = tempdir().unwrap();
    demo(dir.path());
    let path = dir.path().join("run.mcap");
    let data = fs::read(&path).unwrap();
    let mut start = mcap::MAGIC.len();
    let (start, end) = loop {
        let size = u64::from_le_bytes(data[start + 1..start + 9].try_into().unwrap());
        let end = start + 9 + size as usize;
        if data[start] == mcap::records::op::MESSAGE {
            break (start, end);
        }
        start = end;
    };
    let mut corrupt = data[..end].to_vec();
    corrupt.extend_from_slice(&data[start..end]);
    let mut conflicting = data[start..end].to_vec();
    let time = u64::from_le_bytes(conflicting[15..23].try_into().unwrap());
    conflicting[15..23].copy_from_slice(&(time + 1).to_le_bytes());
    corrupt.extend_from_slice(&conflicting);
    fs::write(&path, &corrupt).unwrap();
    let preview = run(recorder().args(["recover", "--input"]).arg(&path));
    assert_eq!(preview["duplicate_identical"], 1);
    assert_eq!(preview["duplicate_conflicting"], 1);
    assert_eq!(preview["committed_with_evidence"], 0);
    let output = dir.path().join("duplicate.mcap");
    let result = recorder()
        .args(["recover", "--apply", "--input"])
        .arg(&path)
        .arg("--output")
        .arg(&output)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!output.exists());
    assert_eq!(fs::read(&path).unwrap(), corrupt);
}
