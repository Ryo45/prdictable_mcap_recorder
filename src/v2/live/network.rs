use std::io;
use std::mem::{size_of, zeroed};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{Result, ensure};

use super::{Backend, Receipt, codec::Metadata};

pub struct Udp {
    socket: UdpSocket,
    port: u16,
}

pub(super) fn option<T>(fd: i32, level: i32, name: i32, value: &T) -> io::Result<()> {
    // setsockopt reads exactly the supplied scalar or ABI structure.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            std::ptr::from_ref(value).cast(),
            size_of::<T>() as _,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

impl Udp {
    pub fn open(bind: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind(bind)?;
        socket.set_nonblocking(true)?;
        let fd = socket.as_raw_fd();
        option(fd, libc::SOL_SOCKET, libc::SO_RXQ_OVFL, &1_i32)?;
        if bind.is_ipv4() {
            option(fd, libc::IPPROTO_IP, libc::IP_PKTINFO, &1_i32)?;
        } else {
            option(fd, libc::IPPROTO_IPV6, libc::IPV6_RECVPKTINFO, &1_i32)?;
        }
        let port = socket.local_addr()?.port();
        Ok(Self { socket, port })
    }
}

impl Backend for Udp {
    fn descriptor(&self) -> i32 {
        self.socket.as_raw_fd()
    }
    fn receive(&mut self, bytes: &mut [u8]) -> Result<Option<Receipt>> {
        // All recvmsg pointers refer to local initialized storage for this call.
        unsafe {
            let mut addr: libc::sockaddr_storage = zeroed();
            let mut control = [0_usize; 32];
            let mut vector = libc::iovec {
                iov_base: bytes.as_mut_ptr().cast(),
                iov_len: bytes.len(),
            };
            let mut msg: libc::msghdr = zeroed();
            msg.msg_name = std::ptr::from_mut(&mut addr).cast();
            msg.msg_namelen = size_of::<libc::sockaddr_storage>() as _;
            msg.msg_iov = &mut vector;
            msg.msg_iovlen = 1;
            msg.msg_control = control.as_mut_ptr().cast();
            msg.msg_controllen = size_of_val(&control);
            let n = libc::recvmsg(self.socket.as_raw_fd(), &mut msg, libc::MSG_TRUNC);
            if n < 0 {
                return no_data();
            }
            let source = match i32::from(addr.ss_family) {
                libc::AF_INET => {
                    let a = &*std::ptr::from_ref(&addr).cast::<libc::sockaddr_in>();
                    SocketAddr::new(
                        IpAddr::from(a.sin_addr.s_addr.to_ne_bytes()),
                        u16::from_be(a.sin_port),
                    )
                }
                libc::AF_INET6 => {
                    let a = &*std::ptr::from_ref(&addr).cast::<libc::sockaddr_in6>();
                    SocketAddr::V6(std::net::SocketAddrV6::new(
                        a.sin6_addr.s6_addr.into(),
                        u16::from_be(a.sin6_port),
                        0,
                        a.sin6_scope_id,
                    ))
                }
                _ => anyhow::bail!("unexpected UDP address family"),
            };
            let mut dest = None;
            let mut interface = 0;
            let mut drops = None;
            let mut item = libc::CMSG_FIRSTHDR(&msg);
            while !item.is_null() {
                let hdr = &*item;
                let data = libc::CMSG_DATA(item);
                let len = hdr.cmsg_len.saturating_sub(libc::CMSG_LEN(0) as usize);
                match (hdr.cmsg_level, hdr.cmsg_type) {
                    (libc::IPPROTO_IP, libc::IP_PKTINFO)
                        if len >= size_of::<libc::in_pktinfo>() =>
                    {
                        let info = data.cast::<libc::in_pktinfo>().read_unaligned();
                        dest = Some(IpAddr::from(info.ipi_addr.s_addr.to_ne_bytes()));
                        interface = info.ipi_ifindex as u32;
                    }
                    (libc::IPPROTO_IPV6, libc::IPV6_PKTINFO)
                        if len >= size_of::<libc::in6_pktinfo>() =>
                    {
                        let info = data.cast::<libc::in6_pktinfo>().read_unaligned();
                        dest = Some(IpAddr::from(info.ipi6_addr.s6_addr));
                        interface = info.ipi6_ifindex;
                    }
                    (libc::SOL_SOCKET, libc::SO_RXQ_OVFL) if len >= size_of::<u32>() => {
                        drops = Some(data.cast::<u32>().read_unaligned())
                    }
                    _ => {}
                }
                item = libc::CMSG_NXTHDR(&msg, item);
            }
            Ok(Some(Receipt {
                metadata: Metadata::Udp {
                    source,
                    dest,
                    interface,
                    port: self.port,
                },
                length: n as usize,
                truncated: msg.msg_flags & libc::MSG_TRUNC != 0 || n as usize > bytes.len(),
                drops,
                source_unix_ns: None,
            }))
        }
    }
}

fn no_data<T>() -> Result<Option<T>> {
    let err = io::Error::last_os_error();
    if matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) {
        Ok(None)
    } else {
        Err(err.into())
    }
}

pub struct Can {
    fd: OwnedFd,
    enable_fd: bool,
}

impl Can {
    pub fn open(interface: &str, enable_fd: bool, errors: bool) -> Result<Self> {
        let name = std::ffi::CString::new(interface)?;
        // Interface name is NUL terminated, and the new socket is uniquely owned.
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        ensure!(index != 0, "CAN interface {interface} not found");
        let fd = unsafe {
            libc::socket(
                libc::AF_CAN,
                libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                1,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if enable_fd {
            option(fd.as_raw_fd(), 101, 5, &1_i32)?;
        }
        if errors {
            option(fd.as_raw_fd(), 101, 2, &0x1fff_ffff_u32)?;
        }
        let mut address: libc::sockaddr_can = unsafe { zeroed() };
        address.can_family = libc::AF_CAN as _;
        address.can_ifindex = index as i32;
        let rc = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                std::ptr::from_ref(&address).cast(),
                size_of::<libc::sockaddr_can>() as _,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(Self { fd, enable_fd })
    }
}

impl Backend for Can {
    fn descriptor(&self) -> i32 {
        self.fd.as_raw_fd()
    }
    fn receive(&mut self, bytes: &mut [u8]) -> Result<Option<Receipt>> {
        let mut raw = [0_u8; 72];
        let mut address: libc::sockaddr_can = unsafe { zeroed() };
        let mut size = size_of::<libc::sockaddr_can>() as libc::socklen_t;
        // recvfrom writes the bounded Linux can_frame/canfd_frame ABI into local storage.
        let got = unsafe {
            libc::recvfrom(
                self.fd.as_raw_fd(),
                raw.as_mut_ptr().cast(),
                raw.len(),
                libc::MSG_TRUNC,
                std::ptr::from_mut(&mut address).cast(),
                &mut size,
            )
        };
        if got < 0 {
            return no_data();
        }
        parse_can(&raw, got, address.can_ifindex as u32, self.enable_fd, bytes).map(Some)
    }
}

fn parse_can(
    raw: &[u8; 72],
    got: isize,
    interface: u32,
    enable_fd: bool,
    bytes: &mut [u8],
) -> Result<Receipt> {
    let fd = got == 72;
    ensure!(
        got == 16 || (fd && enable_fd),
        "invalid SocketCAN frame size"
    );
    let id = u32::from_ne_bytes(raw[..4].try_into()?);
    let length = usize::from(raw[4]);
    ensure!(
        length <= if fd { 64 } else { 8 },
        "invalid CAN payload length"
    );
    let dlc = if fd {
        255
    } else if length == 8 && (9..=15).contains(&raw[7]) {
        raw[7]
    } else {
        raw[4]
    };
    let length = if !fd && id & 0x4000_0000 != 0 {
        0
    } else {
        length
    };
    bytes[..length].copy_from_slice(&raw[8..8 + length]);
    Ok(Receipt {
        metadata: Metadata::Can {
            id,
            interface,
            fd,
            flags: if fd { raw[5] } else { 0 },
            dlc,
        },
        length,
        truncated: false,
        drops: None,
        source_unix_ns: None,
    })
}

#[cfg(test)]
#[test]
fn classical_dlc_and_fd_flags_are_preserved() {
    let mut raw = [0_u8; 72];
    let mut bytes = [0; 64];
    raw[4] = 8;
    raw[7] = 12;
    let classic = parse_can(&raw, 16, 2, true, &mut bytes).unwrap();
    assert!(matches!(
        classic.metadata,
        Metadata::Can {
            dlc: 12,
            fd: false,
            ..
        }
    ));
    raw[4] = 64;
    raw[5] = 3;
    let fd = parse_can(&raw, 72, 2, true, &mut bytes).unwrap();
    assert_eq!(fd.length, 64);
    assert!(matches!(
        fd.metadata,
        Metadata::Can {
            flags: 3,
            fd: true,
            ..
        }
    ));
}
