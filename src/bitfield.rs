use bitvec::bitbox;
use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use derive_more::{Deref, DerefMut};

#[derive(Clone, Deref, DerefMut)]
pub struct BitField {
    bitfield: BitBox<u8, Msb0>
}

impl BitField {
    pub fn new(size: usize) -> Self {
        Self {
            bitfield: bitbox![u8, Msb0; 0; size]
        }
    }
}