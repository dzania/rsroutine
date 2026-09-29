use crate::stack::Stack;

core::arch::global_asm!(include_str!("../asm/aarch_macos.S"));

// Reference: https://github.com/ARM-software/abi-aa/blob/main/aapcs64/aapcs64.rst
#[repr(C)]
#[derive(Debug, Default)]
pub(crate) struct Context {
    /// Stack pointer.
    pub(crate) sp: usize,

    /// Callee-saved general-purpose registers.
    pub(crate) x19: usize,
    pub(crate) x20: usize,
    pub(crate) x21: usize,
    pub(crate) x22: usize,
    pub(crate) x23: usize,
    pub(crate) x24: usize,
    pub(crate) x25: usize,
    pub(crate) x26: usize,
    pub(crate) x27: usize,
    pub(crate) x28: usize,

    /// Frame pointer.
    pub(crate) x29: usize,

    /// Link register / return address.
    pub(crate) x30: usize,
    pub(crate) d8: u64,
    pub(crate) d9: u64,
    pub(crate) d10: u64,
    pub(crate) d11: u64,
    pub(crate) d12: u64,
    pub(crate) d13: u64,
    pub(crate) d14: u64,
    pub(crate) d15: u64,
}

impl Context {
    pub(crate) fn new_routine(stack: &Stack, bootstrap_addr: usize) -> Context {
        let aligned_top = stack.aligned_top();

        Context {
            sp: aligned_top,
            x29: aligned_top,
            x30: bootstrap_addr,
            ..Context::default()
        }
    }
}

// SAFETY: These declarations match the symbols defined in `aarch_macos.S`. `swap_context` reads
// and writes exactly `size_of::<Context>()` bytes through its pointers, using the field offsets of
// the `#[repr(C)]` layout above.
unsafe extern "C" {
    fn swap_context(from: *mut Context, to: *const Context);
    fn bootstrap_entry() -> !;
}

pub(crate) fn bootstrap_entry_addr() -> usize {
    bootstrap_entry as *const () as usize
}

/// Saves the current registers into `from` and resumes execution from `to`.
///
/// Returns only when some later `switch` resumes `from`.
///
/// # Safety
///
/// - `from` must be valid for writes and stay valid until something resumes it.
/// - `to` must point to a `Context` that was either built by `Context::new_routine` for a stack
///   that is still mapped, or saved by an earlier `switch` whose stack is still mapped.
/// - No `&mut` reference that code on the other side of the switch also uses may be live across
///   the call, because that code will create its own references to the same data.
pub(crate) unsafe fn switch(from: *mut Context, to: *const Context) {
    // SAFETY: The caller upholds the contract documented above, which is exactly what
    // `swap_context` needs.
    unsafe {
        swap_context(from, to);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_initializes_stack_registers_and_entry_address() {
        let stack = Stack::new(1024);
        let entry_addr = 0x1234_5678;
        let context = Context::new_routine(&stack, entry_addr);

        assert_eq!(context.sp, stack.aligned_top());
        assert_eq!(context.x29, stack.aligned_top());
        assert_eq!(context.x30, entry_addr);

        assert_eq!(context.x19, 0);
        assert_eq!(context.x20, 0);
        assert_eq!(context.x21, 0);
        assert_eq!(context.x22, 0);
        assert_eq!(context.x23, 0);
        assert_eq!(context.x24, 0);
        assert_eq!(context.x25, 0);
        assert_eq!(context.x26, 0);
        assert_eq!(context.x27, 0);
        assert_eq!(context.x28, 0);
    }
}
