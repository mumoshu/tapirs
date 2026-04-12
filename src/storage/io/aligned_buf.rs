use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ptr::NonNull;

/// 4 KiB NVMe block alignment.
pub const BLOCK_SIZE: usize = 4096;

/// Reusable 4 KiB-aligned buffer for direct I/O.
///
/// `capacity` is always a multiple of `BLOCK_SIZE`.
/// `len` tracks how many bytes of actual data are stored.
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    capacity: usize,
}

// SAFETY: The buffer owns its allocation and has no interior aliasing.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocate a zero-filled buffer with capacity rounded up to `BLOCK_SIZE`.
    pub fn new(min_capacity: usize) -> Self {
        let capacity = round_up(min_capacity.max(BLOCK_SIZE));
        let layout = Layout::from_size_align(capacity, BLOCK_SIZE).unwrap();
        // SAFETY: layout has non-zero size.
        let ptr = unsafe { NonNull::new(alloc_zeroed(layout)).expect("allocation failed") };
        Self {
            ptr,
            len: 0,
            capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Set the logical data length (must be <= capacity).
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity);
        self.len = len;
    }

    /// Copy `data` into the buffer, zero-padding to `capacity`.
    pub fn fill(&mut self, data: &[u8]) {
        assert!(data.len() <= self.capacity);
        // SAFETY: both pointers are valid and non-overlapping.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr(), data.len());
            // Zero the padding region.
            let remaining = self.capacity - data.len();
            if remaining > 0 {
                std::ptr::write_bytes(self.ptr.as_ptr().add(data.len()), 0, remaining);
            }
        }
        self.len = data.len();
    }

    /// Return a slice of the actual data (not the padding).
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for `len` bytes.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Return a slice of the full capacity (including padding).
    pub fn as_full_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.capacity) }
    }

    /// Mutable slice over the full capacity.
    pub fn as_full_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity) }
    }

    fn layout(&self) -> Layout {
        Layout::from_size_align(self.capacity, BLOCK_SIZE).unwrap()
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout()) }
    }
}

/// Round `n` up to the next multiple of `BLOCK_SIZE`.
pub fn round_up(n: usize) -> usize {
    (n + BLOCK_SIZE - 1) & !(BLOCK_SIZE - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_and_fill() {
        let mut buf = AlignedBuf::new(100);
        assert_eq!(buf.capacity(), BLOCK_SIZE);
        assert_eq!(buf.as_ptr() as usize % BLOCK_SIZE, 0);

        buf.fill(b"hello");
        assert_eq!(buf.len(), 5);
        assert_eq!(&buf.as_slice(), b"hello");
        // Padding should be zeroed.
        assert!(buf.as_full_slice()[5..].iter().all(|&b| b == 0));
    }

    #[test]
    fn round_up_values() {
        assert_eq!(round_up(0), 0);
        assert_eq!(round_up(1), BLOCK_SIZE);
        assert_eq!(round_up(BLOCK_SIZE), BLOCK_SIZE);
        assert_eq!(round_up(BLOCK_SIZE + 1), 2 * BLOCK_SIZE);
    }
}
