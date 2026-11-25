use std::io;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

pub static REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn request_stop(_: libc::c_int) {
    REQUESTED.store(true, Relaxed);
}

pub fn install() -> io::Result<()> {
    // The handler's only operation is a lock-free atomic store. Initialize
    // the signal mask with libc and install it before starting worker threads.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = request_stop as *const () as usize;
        action.sa_flags = libc::SA_RESTART;
        if libc::sigemptyset(&mut action.sa_mask) != 0 {
            return Err(io::Error::last_os_error());
        }
        for signal in [libc::SIGINT, libc::SIGTERM] {
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok(())
}
