use super::transform::{Pipeline, Transform};
use anyhow::{Result, ensure};

pub enum Metadata {
    Udp {
        source: std::net::SocketAddr,
        dest: Option<std::net::IpAddr>,
        interface: u32,
        port: u16,
    },
    Can {
        id: u32,
        interface: u32,
        fd: bool,
        flags: u8,
        dlc: u8,
    },
    Ros,
}

pub struct Record<'a> {
    pub metadata: Metadata,
    pub payload: &'a [u8],
}

pub const HEADER: usize = 56;
pub const UDP_ENCODING: &str = "recorder-udp-v1";
pub const CAN_ENCODING: &str = "recorder-socketcan-v1";

pub fn capacity(max: usize, transform: Transform, ros: bool) -> usize {
    if ros {
        max
    } else {
        HEADER
            + match transform {
                Transform::Identity => max,
                Transform::Zstd => zstd::zstd_safe::compress_bound(max),
            }
    }
}

fn address(out: &mut [u8], address: std::net::IpAddr) -> u8 {
    match address {
        std::net::IpAddr::V4(ip) => {
            out[..4].copy_from_slice(&ip.octets());
            4
        }
        std::net::IpAddr::V6(ip) => {
            out.copy_from_slice(&ip.octets());
            6
        }
    }
}

pub fn encode(
    record: Record<'_>,
    transform: Transform,
    pipeline: &mut Pipeline,
    out: &mut [u8],
) -> Result<usize> {
    if matches!(record.metadata, Metadata::Ros) {
        ensure!(
            transform == Transform::Identity,
            "ROS CDR requires identity"
        );
        ensure!(record.payload.len() >= 4, "short ROS CDR");
        return pipeline.apply(record.payload, out);
    }
    let (header, body) = out.split_at_mut(HEADER);
    header.fill(0);
    header[0] = 1;
    header[1] = u8::from(transform == Transform::Zstd);
    header[4..8].copy_from_slice(&(record.payload.len() as u32).to_le_bytes());
    match record.metadata {
        Metadata::Udp {
            source,
            dest,
            interface,
            port,
        } => {
            header[2] = address(&mut header[24..40], source.ip());
            header[3] = dest.map_or(0, |ip| address(&mut header[40..56], ip));
            header[8..12].copy_from_slice(&interface.to_le_bytes());
            header[12..14].copy_from_slice(&source.port().to_le_bytes());
            header[14..16].copy_from_slice(&port.to_le_bytes());
            if let std::net::SocketAddr::V6(source) = source {
                header[16..20].copy_from_slice(&source.scope_id().to_le_bytes());
            }
        }
        Metadata::Can {
            id,
            interface,
            fd,
            flags,
            dlc,
        } => {
            header[2] = u8::from(fd);
            header[3] = flags;
            header[8..12].copy_from_slice(&interface.to_le_bytes());
            header[12..16].copy_from_slice(&id.to_le_bytes());
            header[16] = dlc;
        }
        Metadata::Ros => unreachable!(),
    }
    let length = pipeline.apply(record.payload, body)?;
    Ok(HEADER + length)
}

pub fn decode(encoding: &str, bytes: &[u8], max: usize) -> Result<Vec<u8>> {
    if encoding == "cdr" {
        ensure!((4..=max).contains(&bytes.len()), "invalid CDR length");
        return Ok(bytes.to_vec());
    }
    ensure!(
        bytes.len() >= HEADER && bytes[0] == 1,
        "invalid live codec header"
    );
    let length = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
    ensure!(length <= max, "decoded payload exceeds limit");
    match encoding {
        UDP_ENCODING => ensure!(
            matches!(bytes[2], 4 | 6) && matches!(bytes[3], 0 | 4 | 6),
            "invalid address family"
        ),
        CAN_ENCODING => ensure!(
            matches!(bytes[2], 0 | 1) && length <= if bytes[2] == 1 { 64 } else { 8 },
            "invalid CAN length or form"
        ),
        _ => anyhow::bail!("unsupported live encoding"),
    }
    super::transform::restore(bytes[1], &bytes[HEADER..], length)
}

#[cfg(test)]
#[test]
fn bounded_transform_roundtrips_zero_length_and_binary_datagrams() {
    for bytes in [&[][..], &[9, 0, 3, 0, 255][..]] {
        for mode in [Transform::Identity, Transform::Zstd] {
            let mut output = vec![0; capacity(bytes.len(), mode, false)];
            let mut pipeline = Pipeline::new(mode).unwrap();
            let size = encode(
                Record {
                    metadata: Metadata::Udp {
                        source: "127.0.0.1:1234".parse().unwrap(),
                        dest: Some("127.0.0.1".parse().unwrap()),
                        interface: 1,
                        port: 10000,
                    },
                    payload: bytes,
                },
                mode,
                &mut pipeline,
                &mut output,
            )
            .unwrap();
            assert_eq!(
                decode(UDP_ENCODING, &output[..size], bytes.len()).unwrap(),
                bytes
            );
            assert!(decode(UDP_ENCODING, &output[..size - 1], bytes.len()).is_err());
        }
    }
}
