use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Transform {
    #[default]
    Identity,
    Zstd,
}

pub struct Pipeline(Option<zstd::bulk::Compressor<'static>>);

impl Pipeline {
    pub fn new(mode: Transform) -> Result<Self> {
        Ok(Self(if mode == Transform::Zstd {
            Some(zstd::bulk::Compressor::new(1)?)
        } else {
            None
        }))
    }

    pub fn apply(&mut self, input: &[u8], output: &mut [u8]) -> Result<usize> {
        if let Some(compressor) = &mut self.0 {
            Ok(compressor.compress_to_buffer(input, output)?)
        } else {
            ensure!(output.len() >= input.len(), "identity output exceeds bound");
            output[..input.len()].copy_from_slice(input);
            Ok(input.len())
        }
    }
}

pub fn restore(code: u8, body: &[u8], length: usize) -> Result<Vec<u8>> {
    let decoded = match code {
        0 => body.to_vec(),
        1 => zstd::bulk::decompress(body, length)?,
        _ => anyhow::bail!("unsupported transform"),
    };
    ensure!(decoded.len() == length, "decoded payload length mismatch");
    Ok(decoded)
}
