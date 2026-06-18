/// Owned memory region
struct Stack {
    bytes: Vec<u8>,
}

impl Stack {
    fn new(size: usize) -> Self {
        Stack {
            bytes: vec![0; size],
        }
    }

    // return address of the first byte
    fn bottom_addr(&self) -> usize {
        self.bytes.as_ptr() as usize
    }

    fn top_addr(&self) -> usize {
        self.bottom_addr() + self.bytes.len()
    }

    fn aligned_top(&self) -> usize {
        let top_addr = self.top_addr();
        top_addr - (top_addr % 16)
    }
}

// TODO: This is Apple Silicon hide it behind a feature flag
// Reference: https://github.com/ARM-software/abi-aa/blob/main/aapcs64/aapcs64.rst
#[repr(C)]
#[derive(Debug, Default)]
pub struct Context {
    /// Stack pointer.
    pub sp: usize,

    /// Callee-saved general-purpose registers.
    pub x19: usize,
    pub x20: usize,
    pub x21: usize,
    pub x22: usize,
    pub x23: usize,
    pub x24: usize,
    pub x25: usize,
    pub x26: usize,
    pub x27: usize,
    pub x28: usize,

    /// Frame pointer.
    pub x29: usize,

    /// Link register / return address.
    pub x30: usize,
}

impl Context {
    fn new(stack: &Stack, entry_addr: usize) -> Context {
        Context {
            sp: stack.top_addr(),
            x30: entry_addr,
            ..Context::default()
        }
    }
}

fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn create_context(func: &dyn FnOnce() -> ()) {}
