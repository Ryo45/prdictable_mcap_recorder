use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;

use super::super::integrity::CommitLine;
use crate::metrics::ns;

pub(super) struct Journal {
    output: Option<BufWriter<File>>,
    confirmed_end: u64,
    pub(super) report: JournalReport,
}

#[derive(Clone, Copy, Default, Serialize)]
pub struct JournalReport {
    pub write_ns: u64,
    pub flush_ns: u64,
    pub sync_ns: u64,
    pub failures: u64,
    pub rollback_failed: bool,
}

impl Journal {
    pub(super) fn new(file: File) -> Self {
        Self {
            output: Some(BufWriter::new(file)),
            confirmed_end: 0,
            report: JournalReport::default(),
        }
    }

    pub(super) fn append(&mut self, line: &CommitLine) -> Result<()> {
        let start = Instant::now();
        let output = self.output.as_mut().unwrap();
        let result = (|| {
            serde_json::to_writer(&mut *output, line)?;
            output.write_all(b"\n")?;
            Ok(())
        })();
        self.report.write_ns += ns(start.elapsed());
        result
    }

    pub(super) fn sync(&mut self) -> Result<()> {
        let start = Instant::now();
        let output = self.output.as_mut().unwrap();
        output.flush()?;
        self.report.flush_ns += ns(start.elapsed());
        let start = Instant::now();
        let result = output.get_ref().sync_data();
        self.report.sync_ns += ns(start.elapsed());
        result?;
        self.confirmed_end = output.get_ref().metadata()?.len();
        Ok(())
    }
}

impl Journal {
    pub(super) fn discard_unconfirmed(&mut self) {
        if let Some(output) = self.output.take() {
            let (file, _) = output.into_parts();
            if file
                .set_len(self.confirmed_end)
                .and_then(|()| file.sync_data())
                .is_err()
            {
                self.report.rollback_failed = true;
            }
        }
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Groups are flushed only by the explicit sync path.
        if let Some(output) = self.output.take() {
            let _ = output.into_parts();
        }
    }
}
