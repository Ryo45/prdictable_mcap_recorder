use std::io::{ErrorKind, Read};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};

use super::{Backend, Receipt, codec::Metadata};

pub struct Ros {
    child: Child,
    pipe: ChildStdout,
    header: [u8; 12],
    header_read: usize,
    payload_read: usize,
}

impl Ros {
    pub fn open(
        helper: &Path,
        topic: &str,
        kind: &str,
        qos: &str,
        max: usize,
    ) -> Result<(Self, Vec<u8>)> {
        let mut child = Command::new("python3")
            .arg(helper)
            .args([topic, kind, qos, &max.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .context("launch ROS 2 bridge")?;
        let pipe = child.stdout.take().unwrap();
        let mut bridge = Self {
            child,
            pipe,
            header: [0; 12],
            header_read: 0,
            payload_read: 0,
        };
        let fd = bridge.pipe.as_raw_fd();
        // Only the read side is nonblocking; the bounded pipe supplies bridge backpressure.
        let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) };
        if rc < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut schema = vec![0; 1024 * 1024];
        let limit = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some((size, truncated, _)) = bridge.packet(&mut schema)? {
                ensure!(!truncated && size > 0, "invalid ROS schema handshake");
                schema.truncate(size);
                std::str::from_utf8(&schema)?;
                return Ok((bridge, schema));
            }
            ensure!(
                Instant::now() < limit,
                "ROS 2 schema/subscription setup timed out"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn packet(&mut self, buffer: &mut [u8]) -> Result<Option<(usize, bool, u64)>> {
        while self.header_read < self.header.len() {
            let Some(count) = part(&mut self.pipe, &mut self.header[self.header_read..])? else {
                return Ok(None);
            };
            self.header_read += count;
        }
        let size = u32::from_le_bytes(self.header[..4].try_into()?);
        if size == u32::MAX {
            self.header_read = 0;
            return Ok(Some((0, true, 0)));
        }
        let size = size as usize;
        ensure!(
            size <= buffer.len(),
            "ROS bridge frame exceeds configured bound"
        );
        while self.payload_read < size {
            let Some(count) = part(&mut self.pipe, &mut buffer[self.payload_read..size])? else {
                return Ok(None);
            };
            self.payload_read += count;
        }
        self.header_read = 0;
        self.payload_read = 0;
        Ok(Some((
            size,
            false,
            u64::from_le_bytes(self.header[4..].try_into()?),
        )))
    }
}

fn part(pipe: &mut ChildStdout, target: &mut [u8]) -> Result<Option<usize>> {
    match pipe.read(target) {
        Ok(0) => anyhow::bail!("ROS bridge exited or closed stdout"),
        Ok(n) => Ok(Some(n)),
        Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

impl Backend for Ros {
    fn descriptor(&self) -> i32 {
        self.pipe.as_raw_fd()
    }
    fn receive(&mut self, bytes: &mut [u8]) -> Result<Option<Receipt>> {
        Ok(self
            .packet(bytes)?
            .map(|(length, truncated, stamp)| Receipt {
                metadata: Metadata::Ros,
                length,
                truncated,
                drops: None,
                source_unix_ns: (stamp != 0).then_some(stamp),
            }))
    }
}

impl Drop for Ros {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
