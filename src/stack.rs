use std::ptr::NonNull;

use libc::{_SC_PAGESIZE, MAP_FAILED};

pub(crate) struct Stack {
    mapping: NonNull<u8>,
    mapping_len: usize,
    guard_len: usize,
    usable_len: usize,
}

// SAFETY: Stack exclusively owns mmap allocation
unsafe impl Send for Stack {}

impl Stack {
    pub(crate) fn new(size: usize) -> Self {
        let page_size = page_size();
        let usable_len = round_up_to_page(size, page_size).expect("stack size overflow");
        let guard_len = page_size;
        let mapping_len = guard_len
            .checked_add(usable_len)
            .expect("stack mapping size overflow");
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapping_len,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        if raw == MAP_FAILED {
            panic!("Failed to allocate stack");
        };

        let mapping = NonNull::new(raw.cast::<u8>()).expect("mmap returned null");
        let stack = Self {
            mapping,
            mapping_len,
            usable_len,
            guard_len,
        };

        let result = unsafe {
            libc::mprotect(
                stack.bottom_addr() as *mut libc::c_void,
                stack.usable_len,
                libc::PROT_READ | libc::PROT_WRITE,
            )
        };
        // Panicking drops `stack`, whose Drop impl unmaps the region.
        if result != 0 {
            panic!("Failed to allocate protected page.")
        };
        stack
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.usable_len
    }

    pub(crate) fn bottom_addr(&self) -> *mut u8 {
        unsafe { self.mapping.as_ptr().add(self.guard_len) }
    }

    pub(crate) fn top_addr(&self) -> usize {
        self.bottom_addr() as usize + self.usable_len
    }

    pub(crate) fn aligned_top(&self) -> usize {
        let top_addr = self.top_addr();
        top_addr - (top_addr % 16)
    }
}

fn round_up_to_page(size: usize, page_size: usize) -> Option<usize> {
    assert!(page_size > 0);

    let remainder = size % page_size;

    if remainder == 0 {
        Some(size)
    } else {
        size.checked_add(page_size - remainder)
    }
}

fn page_size() -> usize {
    let size = unsafe { libc::sysconf(_SC_PAGESIZE) };

    assert!(size > 0, "failed to obtain system page size");

    size as usize
}

impl Drop for Stack {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(
                self.mapping.as_ptr().cast::<libc::c_void>(),
                self.mapping_len,
            )
        };
    }
}
