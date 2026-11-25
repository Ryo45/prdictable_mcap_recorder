use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

use anyhow::{Context, Result};
use io_uring::{IoUring, opcode, types};

use super::adapter::Frame;
use super::config::{Faults, IoFault, US, WriterMode};
use super::layout::MESSAGE_HEADER_BYTES;

pub struct Request {
    pub id: u64,
    pub offset: u64,
    pub header: [u8; MESSAGE_HEADER_BYTES],
    pub frame: Frame,
    pub submitted_ns: u64,
}

impl Request {
    pub fn len(&self) -> usize {
        self.header.len() + self.frame.size
    }
}

pub struct Completion {
    pub request: Request,
    pub result: io::Result<()>,
}

pub fn probe(mode: WriterMode) -> Result<()> {
    if mode != WriterMode::IoUring {
        return Ok(());
    }
    let mut ring = IoUring::new(2).context(
        "create io_uring (use writer=sync or mock-async in an environment that restricts io_uring)",
    )?;
    // The NOP has no pointers or lifetime requirements.
    unsafe { ring.submission().push(&opcode::Nop::new().build()) }
        .map_err(|_| anyhow::anyhow!("io_uring probe queue full"))?;
    ring.submit_and_wait(1).context("submit io_uring probe")?;
    let result = ring
        .completion()
        .next()
        .context("io_uring probe completion missing")?
        .result();
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result)).context("io_uring probe completion");
    }
    Ok(())
}

pub struct Backend {
    file: Option<File>,
    faults: Faults,
    inner: Mode,
}

enum Mode {
    Sync(Option<Completion>),
    Mock {
        pending: Vec<MockRequest>,
        capacity: usize,
    },
    Ring(Ring),
}

struct MockRequest {
    request: Request,
    ready_ns: u64,
    cancelled: bool,
}

impl Backend {
    pub fn new(mode: WriterMode, depth: usize, file: Option<File>, faults: Faults) -> Result<Self> {
        let inner = match mode {
            WriterMode::Sync | WriterMode::Null => Mode::Sync(None),
            WriterMode::MockAsync => Mode::Mock {
                pending: Vec::with_capacity(depth),
                capacity: depth,
            },
            WriterMode::IoUring => Mode::Ring(Ring::new(depth)?),
        };
        Ok(Self {
            file,
            faults,
            inner,
        })
    }

    pub fn submit(&mut self, request: Request, now: u64) -> io::Result<()> {
        match &mut self.inner {
            Mode::Sync(ready) => {
                assert!(ready.is_none());
                let result = write_request(self.file.as_ref(), &self.faults, &request);
                *ready = Some(Completion { request, result });
                Ok(())
            }
            Mode::Mock { pending, capacity } => {
                assert!(pending.len() < *capacity);
                let delay = self.faults.mock_delay_us * US;
                let ready_ns = now.saturating_add(if request.id.is_multiple_of(2) {
                    delay.saturating_mul(2)
                } else {
                    delay
                });
                pending.push(MockRequest {
                    request,
                    ready_ns,
                    cancelled: false,
                });
                Ok(())
            }
            Mode::Ring(ring) => ring.submit(self.file.as_ref().unwrap(), &self.faults, request),
        }
    }

    pub fn completion(&mut self, now: u64) -> io::Result<Option<Completion>> {
        match &mut self.inner {
            Mode::Sync(ready) => Ok(ready.take()),
            Mode::Mock { pending, .. } => {
                let Some(index) = pending.iter().rposition(|waiting| waiting.ready_ns <= now)
                else {
                    return Ok(None);
                };
                let waiting = pending.swap_remove(index);
                let result = if waiting.cancelled {
                    Err(io::Error::from_raw_os_error(libc::ECANCELED))
                } else {
                    write_request(self.file.as_ref(), &self.faults, &waiting.request)
                };
                Ok(Some(Completion {
                    request: waiting.request,
                    result,
                }))
            }
            Mode::Ring(ring) => ring.complete(self.file.as_ref().unwrap(), &self.faults),
        }
    }

    pub fn write_control(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if let Some(file) = &self.file {
            file.write_all_at(bytes, offset)?;
        }
        Ok(())
    }

    pub fn sync(&self, group: u64) -> io::Result<()> {
        if self.faults.sync_fail_group == Some(group) {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        if let Some(file) = &self.file {
            file.sync_data()?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn request_cancel(&mut self, id: u64) {
        let Mode::Mock { pending, .. } = &mut self.inner else {
            panic!("test requires MockAsync");
        };
        pending
            .iter_mut()
            .find(|waiting| waiting.request.id == id)
            .unwrap()
            .cancelled = true;
    }
}

fn injected_error(faults: &Faults, id: u64) -> Option<io::Error> {
    if faults.io_error_request != Some(id) {
        return None;
    }
    faults.io_error.map(|fault| {
        io::Error::from_raw_os_error(match fault {
            IoFault::Eio => libc::EIO,
            IoFault::Enospc => libc::ENOSPC,
        })
    })
}

fn write_request(file: Option<&File>, faults: &Faults, request: &Request) -> io::Result<()> {
    if let Some(file) = file {
        write_all(file, request.offset, &request.header, faults.io_short_bytes)?;
    }
    if let Some(error) = injected_error(faults, request.id) {
        return Err(error);
    }
    if let Some(file) = file {
        write_all(
            file,
            request.offset + request.header.len() as u64,
            request.frame.bytes(),
            faults.io_short_bytes,
        )?;
    }
    Ok(())
}

fn write_all(file: &File, mut offset: u64, mut data: &[u8], cap: Option<usize>) -> io::Result<()> {
    if cap.is_none() {
        return file.write_all_at(data, offset);
    }
    while !data.is_empty() {
        let prefix = &data[..data.len().min(cap.unwrap())];
        match file.write_at(prefix, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(count) => {
                offset += count as u64;
                data = &data[count..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

struct RingSlot {
    request: Option<Request>,
    consumed: usize,
    submitted_bytes: usize,
    wire: Box<RingWire>,
    local_result: Option<io::Result<()>>,
}

struct RingWire {
    header: [u8; MESSAGE_HEADER_BYTES],
    iov: [libc::iovec; 2],
}

struct Ring {
    ring: IoUring,
    // This allocation never moves or resizes. It owns headers and iovecs until
    // an original completion confirms that the kernel no longer reads them.
    slots: Vec<RingSlot>,
}

impl Ring {
    fn new(depth: usize) -> Result<Self> {
        let ring = IoUring::new(depth as u32).context("open io_uring backend")?;
        let slots = (0..depth)
            .map(|_| RingSlot {
                request: None,
                consumed: 0,
                submitted_bytes: 0,
                wire: Box::new(RingWire {
                    header: [0; MESSAGE_HEADER_BYTES],
                    iov: [libc::iovec {
                        iov_base: std::ptr::null_mut(),
                        iov_len: 0,
                    }; 2],
                }),
                local_result: None,
            })
            .collect();
        Ok(Self { ring, slots })
    }

    fn submit(&mut self, file: &File, faults: &Faults, request: Request) -> io::Result<()> {
        let index = self
            .slots
            .iter()
            .position(|slot| slot.request.is_none())
            .expect("bounded in-flight admission");
        let slot = &mut self.slots[index];
        slot.wire.header = request.header;
        slot.request = Some(request);
        slot.consumed = 0;
        self.send_suffix(index, file, faults)
    }

    fn send_suffix(&mut self, index: usize, file: &File, faults: &Faults) -> io::Result<()> {
        let slot = &mut self.slots[index];
        let request = slot.request.as_ref().unwrap();
        let header_len = request.header.len();
        let attempted_fault = injected_error(faults, request.id);
        if slot.consumed >= header_len
            && let Some(error) = attempted_fault
        {
            slot.local_result = Some(Err(error));
            return Ok(());
        }
        let mut remaining = faults.io_short_bytes.unwrap_or(usize::MAX);
        if attempted_fault.is_some() {
            remaining = remaining.min(header_len - slot.consumed);
        }
        // Linux caps one vectored write below 2 GiB. A shorter completion is
        // continued from the exact suffix, using the original request ID.
        remaining = remaining.min(0x7fff_f000);
        let mut len = 0;
        let header = &slot.wire.header[slot.consumed.min(header_len)..];
        let payload = &request.frame.bytes()[slot.consumed.saturating_sub(header_len)..];
        slot.submitted_bytes = 0;
        for data in [header, payload] {
            let n = remaining.min(data.len());
            if n != 0 {
                slot.wire.iov[len] = libc::iovec {
                    iov_base: data.as_ptr().cast_mut().cast(),
                    iov_len: n,
                };
                len += 1;
                remaining -= n;
                slot.submitted_bytes += n;
            }
        }
        assert!(len != 0);
        let entry = opcode::Writev::new(
            types::Fd(file.as_raw_fd()),
            slot.wire.iov.as_ptr(),
            len as u32,
        )
        .offset(request.offset + slot.consumed as u64)
        .build()
        .user_data(index as u64);
        // SAFETY: the non-moving slot owns the iovec, its header, and the
        // Lease. The writer cannot take the slot or return the Lease before
        // the original CQE. The ranges of different requests are disjoint.
        unsafe { self.ring.submission().push(&entry) }
            .map_err(|_| io::Error::other("io_uring submission queue full"))?;
        loop {
            match self.ring.submit() {
                Ok(n) if n > 0 => return Ok(()),
                Ok(_) => {
                    return Err(io::Error::other(
                        "io_uring did not consume queued submission",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn complete(&mut self, file: &File, faults: &Faults) -> io::Result<Option<Completion>> {
        for slot in &mut self.slots {
            if let Some(result) = slot.local_result.take() {
                return Ok(Some(Completion {
                    request: slot.request.take().unwrap(),
                    result,
                }));
            }
        }
        loop {
            let next = { self.ring.completion().next() };
            let Some(next) = next else {
                return Ok(None);
            };
            let index = next.user_data() as usize;
            let result = next.result();
            let slot = &mut self.slots[index];
            let request = slot.request.as_ref().expect("CQE without live request");
            if result == -libc::EINTR {
                self.send_suffix(index, file, faults)?;
                continue;
            }
            if result > 0 && result as usize <= slot.submitted_bytes {
                slot.consumed += result as usize;
                if slot.consumed < request.len() {
                    self.send_suffix(index, file, faults)?;
                    continue;
                }
                return Ok(Some(Completion {
                    request: slot.request.take().unwrap(),
                    result: Ok(()),
                }));
            }
            let error = if result < 0 {
                io::Error::from_raw_os_error(-result)
            } else if result == 0 {
                io::ErrorKind::WriteZero.into()
            } else {
                io::Error::other("io_uring reported more bytes than requested")
            };
            return Ok(Some(Completion {
                request: slot.request.take().unwrap(),
                result: Err(error),
            }));
        }
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        // A fatal ring-level error or unwinding can leave the kernel's access
        // unconfirmed. Preserve the allocation and its leases for the rest of
        // this process rather than hand their memory back to a producer.
        if self.slots.iter().any(|slot| slot.request.is_some()) {
            std::mem::forget(std::mem::take(&mut self.slots));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::adapter::{FrameId, Payload};
    use crate::v2::pool::{Pool, ReturnPolicy};
    use std::time::Instant;

    #[test]
    fn out_of_order_completion_and_cancel_hold_the_original_leases() {
        let pool = Pool::new(0, 2, 0, 40, Instant::now(), ReturnPolicy::default()).unwrap();
        let file = tempfile::tempfile().unwrap();
        let mut backend = Backend::new(
            WriterMode::MockAsync,
            2,
            Some(file),
            Faults {
                mock_delay_us: 10,
                ..Faults::default()
            },
        )
        .unwrap();
        for id in 0..2 {
            backend
                .submit(
                    Request {
                        id,
                        offset: id * 100,
                        header: [0; 31],
                        submitted_ns: 0,
                        frame: Frame {
                            id: FrameId {
                                source: 0,
                                session_epoch: 1,
                                sequence: id,
                            },
                            planned_ns: 0,
                            captured_ns: 0,
                            queued_ns: 0,
                            size: 40,
                            payload: Payload::Borrowed(pool.acquire(0).unwrap().0),
                        },
                    },
                    0,
                )
                .unwrap();
        }
        backend.request_cancel(0);
        assert!(pool.acquire(0).is_none());
        let second = backend.completion(10_000).unwrap().unwrap();
        assert_eq!(second.request.id, 1);
        assert!(second.result.is_ok());
        assert!(pool.acquire(0).is_none());
        drop(second);
        assert_eq!(pool.levels(u64::MAX).available, 1);
        assert!(backend.completion(19_999).unwrap().is_none());
        let first = backend.completion(20_000).unwrap().unwrap();
        assert_eq!(first.request.id, 0);
        assert_eq!(
            first.result.unwrap_err().raw_os_error(),
            Some(libc::ECANCELED)
        );
        assert_eq!(pool.levels(u64::MAX).available, 1);
        drop(first.request);
        assert_eq!(pool.levels(u64::MAX).available, 2);
    }
}
