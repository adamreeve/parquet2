use std::marker::PhantomData;
use crate::error::Error;
use crate::types::NativeType;

/// Decodes according to [Byte Stream Split](https://github.com/apache/parquet-format/blob/master/Encodings.md#byte-stream-split-byte_stream_split--9).
/// # Implementation
/// This struct does not allocate on the heap.
#[derive(Debug)]
pub struct Decoder<'a, T: NativeType, const S: usize> {
    values: &'a [u8],
    buffer: [u8; S],
    num_elements: usize,
    current: usize,
    element_type: PhantomData<T>
}

impl<'a, T: NativeType, const S: usize> Decoder<'a, T, S> {
    pub fn try_new(values: &'a [u8]) -> Result<Self, Error> {
        let element_size = std::mem::size_of::<T>();
        if element_size != S {
            return Err(Error::oos(format!(
                "Element size {element_size} does not match the size type parameter {S}"
            )));
        }
        let values_size = values.len();
        if values_size % S != 0 {
            return Err(Error::oos("Value array is not a multiple of element size"));
        }
        let num_elements = values.len() / S;
        Ok(Self {
            values,
            buffer: [0_u8; S],
            num_elements,
            current: 0,
            element_type: PhantomData
        })
    }
}

impl<'a, T: NativeType, const S: usize> Iterator for Decoder<'a, T, S> {
    type Item = Result<T, Error>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.num_elements {
            return None
        }

        for n in 0..S {
            self.buffer[n] = self.values[(self.num_elements * n) + self.current]
        }

        let value = T::from_le_bytes(self.buffer.as_slice().try_into().unwrap());

        self.current += 1;

        return Some(Ok(value));
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.num_elements, Some(self.num_elements))
    }
}
