use std::marker::PhantomData;
use tokio_util::bytes::{Bytes, BytesMut};

#[derive(Clone)]
pub struct BincodeCodec<E, D> {
    config: bincode::config::Configuration,
    length_codec: tokio_util::codec::LengthDelimitedCodec,
    e: PhantomData<E>,
    d: PhantomData<D>,
}
impl<E, D> Default for BincodeCodec<E, D> {
    fn default() -> Self {
        BincodeCodec {
            config: bincode::config::standard(),
            length_codec: tokio_util::codec::LengthDelimitedCodec::builder()
                .length_field_length(4)
                .max_frame_length(u32::MAX as usize)
                .new_codec(),
            e: PhantomData,
            d: PhantomData,
        }
    }
}

impl<E, D: bincode::Decode<()>> tokio_util::codec::Decoder for BincodeCodec<E, D> {
    type Item = D;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let frame = match self.length_codec.decode(src).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to decode length-delimited data",
            )
        })? {
            Some(frame) => frame,
            None => return Ok(None),
        };
        let (message, _): (Self::Item, _) = bincode::decode_from_slice(&frame, self.config)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to decode length-delimited data",
                )
            })?;
        Ok(Some(message))
    }
}

pub struct EncodeError<M> {
    pub io_error: std::io::Error,
    pub msg: Option<M>,
}

pub trait ErrWithMsg<M> {
    fn into_inner(self) -> Option<M>;
}

impl<M> ErrWithMsg<M> for EncodeError<M> {
    fn into_inner(self) -> Option<M> {
        self.msg
    }
}

impl<M> From<std::io::Error> for EncodeError<M> {
    fn from(err: std::io::Error) -> Self {
        EncodeError {
            io_error: err,
            msg: None,
        }
    }
}

impl<E: bincode::Encode, D> tokio_util::codec::Encoder<E> for BincodeCodec<E, D> {
    type Error = EncodeError<E>;
    fn encode(&mut self, item: E, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_data = bincode::encode_to_vec(&item, self.config).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to encode data")
        })?;
        self.length_codec
            .encode(Bytes::from(encoded_data), dst)
            .map_err(|_| EncodeError {
                io_error: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Couldn't encode length-delimited data",
                ),
                msg: Some(item),
            })?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct BincodeSubdecoder<D, MD> {
    config: bincode::config::Configuration,
    length_codec: tokio_util::codec::LengthDelimitedCodec,
    d: PhantomData<D>,
    md: PhantomData<MD>,
}
impl<D, MD> Default for BincodeSubdecoder<D, MD> {
    fn default() -> Self {
        BincodeSubdecoder {
            config: bincode::config::standard(),
            length_codec: tokio_util::codec::LengthDelimitedCodec::builder()
                .length_field_length(4)
                .max_frame_length(u32::MAX as usize)
                .new_codec(),
            d: PhantomData,
            md: PhantomData,
        }
    }
}
impl<MD: From<D>, D: bincode::Decode<()>> tokio_util::codec::Decoder for BincodeSubdecoder<D, MD> {
    type Item = MD;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let frame = match self.length_codec.decode(src).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Failed to decode length-delimited data",
            )
        })? {
            Some(frame) => frame,
            None => return Ok(None),
        };
        let (message, _): (D, _) =
            bincode::decode_from_slice(&frame, self.config).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to decode length-delimited data",
                )
            })?;
        Ok(Some(message.into()))
    }
}
