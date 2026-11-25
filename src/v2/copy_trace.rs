use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::adapter::Frame;

const MAX_EVENTS: u64 = 32_768;

pub struct CopyTrace(Mutex<State>);

struct State {
    file: BufWriter<File>,
    written: u64,
    omitted: u64,
    failed: bool,
}

impl CopyTrace {
    pub fn open(data: &Path) -> Result<Option<Self>> {
        if std::env::var_os("RECORDER_COPY_TRACE").as_deref() != Some("1".as_ref()) {
            return Ok(None);
        }
        let mut name = data
            .file_stem()
            .context("copy trace needs a file name")?
            .to_os_string();
        name.push("-copy-trace.jsonl");
        Ok(Some(Self(Mutex::new(State {
            file: BufWriter::with_capacity(
                64 * 1024,
                crate::report::create_new(&data.with_file_name(name))?,
            ),
            written: 0,
            omitted: 0,
            failed: false,
        }))))
    }

    pub fn frame(&self, event: &str, now: u64, frame: &Frame, detail: Value) {
        let mut state = self.0.lock().unwrap();
        if state.failed || state.written >= MAX_EVENTS {
            state.omitted += 1;
            return;
        }
        let value = json!({
            "event": event, "at_ns": now, "stream": frame.id.source,
            "sequence": frame.id.sequence, "epoch": frame.id.session_epoch,
            "size_bytes": frame.size, "queued_ns": frame.queued_ns,
            "lease_deadline_ns": frame.borrowed().and_then(|lease| lease.deadline_ns),
            "detail": detail,
        });
        state.failed = serde_json::to_writer(&mut state.file, &value).is_err()
            || state.file.write_all(b"\n").is_err();
        if !state.failed {
            state.written += 1;
        }
    }

    pub fn finish(self) {
        let mut state = self.0.into_inner().unwrap();
        let footer = json!({"event":"trace_end", "written":state.written, "omitted":state.omitted, "failed":state.failed});
        let result = serde_json::to_writer(&mut state.file, &footer).is_ok()
            && state.file.write_all(b"\n").is_ok()
            && state.file.flush().is_ok();
        if !result || state.failed || state.omitted != 0 {
            eprintln!("copy trace incomplete ({} omitted)", state.omitted);
        }
    }
}
