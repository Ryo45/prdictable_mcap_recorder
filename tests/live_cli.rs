use std::fs;
use std::process::Command;

#[test]
fn bridge_protocol_preserves_cdr_and_uses_live_provenance() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::write(
        root.join("bridge.py"),
        r#"
import struct, sys, time
def send(data, stamp):
    sys.stdout.buffer.write(struct.pack("<IQ", len(data), stamp) + data)
    sys.stdout.buffer.flush()
send(b"string data\n", 0)
send(b"x", 0)
send(b"\x00\x01\x00\x00\x03\x00\x00\x00hi\x00", 42)
time.sleep(3)
"#,
    )
    .unwrap();
    fs::write(
        root.join("live.toml"),
        r#"
[durability]
mode="overlap"
commit_messages=1

[[sources]]
id="text"
kind="ros2"
topic="/fixture/text"
message_type="std_msgs/msg/String"
bridge="bridge.py"
max_payload_bytes=1024
"#,
    )
    .unwrap();
    let launch = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_recorder"))
            .current_dir(root)
            .args(args)
            .output()
            .unwrap()
    };
    let result = launch(&[
        "record-live",
        "--config",
        "live.toml",
        "--output",
        "run.mcap",
        "--duration-sec",
        "1",
    ]);
    assert!(result.status.success(), "{result:?}");
    let bytes = fs::read(root.join("run.mcap")).unwrap();
    let messages = mcap::MessageStream::new(&bytes)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].sequence, 1);
    assert!(
        fs::read_to_string(root.join("run-loss.jsonl"))
            .unwrap()
            .contains("transform_failure")
    );
    assert_eq!(messages[0].publish_time, 42);
    assert_eq!(messages[0].channel.message_encoding, "cdr");
    assert_eq!(
        messages[0].data.as_ref(),
        b"\x00\x01\x00\x00\x03\x00\x00\x00hi\x00"
    );
    for cmd in ["verify-live", "recover"] {
        let result = launch(&[cmd, "--input", "run.mcap"]);
        assert!(result.status.success(), "{result:?}");
    }
    let last_byte = bytes
        .windows(3)
        .position(|window| window == b"hi\x00")
        .unwrap();
    let mut broken = bytes;
    broken[last_byte] ^= 1;
    fs::write(root.join("run.mcap"), broken).unwrap();
    assert!(
        !launch(&["verify-live", "--input", "run.mcap"])
            .status
            .success()
    );
}

#[test]
fn compressed_ros_is_rejected_before_opening_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::write(
        root.join("live.toml"),
        r#"
[[sources]]
id="text"
kind="ros2"
topic="/fixture/text"
message_type="std_msgs/msg/String"
transform="zstd"
"#,
    )
    .unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_recorder"))
        .current_dir(root)
        .args([
            "record-live",
            "--config",
            "live.toml",
            "--output",
            "run.mcap",
        ])
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!root.join("run.mcap").exists());
}

#[test]
fn receive_failure_is_reported_separately_from_observed_frame_loss() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::write(
        root.join("bridge.py"),
        r#"
import struct, sys
data = b"string data\n"
sys.stdout.buffer.write(struct.pack("<IQ", len(data), 0) + data)
"#,
    )
    .unwrap();
    fs::write(
        root.join("live.toml"),
        r#"
[[sources]]
id="text"
kind="ros2"
topic="/fixture/text"
message_type="std_msgs/msg/String"
bridge="bridge.py"
max_payload_bytes=1024
"#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_recorder"))
        .current_dir(root)
        .args([
            "record-live",
            "--config",
            "live.toml",
            "--output",
            "run.mcap",
            "--duration-sec",
            "1",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let summary: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("run-summary.json")).unwrap()).unwrap();
    assert!(summary["sources"][0]["receive"]["receive_error"].is_string());
    assert_eq!(summary["sources"][0]["counts"]["before_accept_loss"], 0);
    assert_eq!(summary["writer"]["final_sync"], true);
}

#[test]
fn schema_handshake_failure_leaves_no_mcap() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::write(root.join("bridge.py"), "raise SystemExit(1)\n").unwrap();
    fs::write(
        root.join("live.toml"),
        r#"
[[sources]]
id="text"
kind="ros2"
topic="/fixture/text"
message_type="std_msgs/msg/String"
bridge="bridge.py"
"#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_recorder"))
        .current_dir(root)
        .args([
            "record-live",
            "--config",
            "live.toml",
            "--output",
            "run.mcap",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!root.join("run.mcap").exists());
}
