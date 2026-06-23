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
}

impl Context {
    pub(crate) fn new(stack: &Stack, entry_addr: usize) -> Context {
        let aligned_top = stack.aligned_top();

        Context {
            sp: aligned_top,
            x29: aligned_top,
            x30: entry_addr,
            ..Context::default()
        }
    }
}

unsafe extern "C" {
    fn swap_context(from: *mut Context, to: *const Context);
    fn bootstrap_entry() -> !;
}

pub(crate) fn bootstrap_entry_addr() -> usize {
    bootstrap_entry as *const () as usize
}

pub(crate) unsafe fn switch(from: &mut Context, to: &Context) {
    // SAFETY: The caller guarantees that both references point to valid Context values, that
    // `to.sp` points to a valid stack, and that `to.x30` points to executable code.
    unsafe {
        swap_context(from as *mut Context, to as *const Context);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_initializes_stack_registers_and_entry_address() {
        let stack = Stack::new(1024);
        let entry_addr = 0x1234_5678;
        let context = Context::new(&stack, entry_addr);

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
