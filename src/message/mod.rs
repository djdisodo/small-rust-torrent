use num_enum::TryFromPrimitive;

#[derive(Eq, PartialEq, Debug, Copy, Clone, TryFromPrimitive)]
#[repr(u8)]
pub enum Message {
    Choke = 0,
    UnChoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    BitField = 5,
    Request = 6,
    Piece = 7
}