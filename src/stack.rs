// TODO: Implement stack guard page.
pub(crate) struct Stack {
    bytes: Vec<u8>,
}

impl Stack {
    pub(crate) fn new(size: usize) -> Self {
        Stack {
            bytes: vec![0; size],
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn bottom_addr(&self) -> usize {
        self.bytes.as_ptr() as usize
    }

    pub(crate) fn top_addr(&self) -> usize {
        self.bottom_addr() + self.bytes.len()
    }

    pub(crate) fn aligned_top(&self) -> usize {
        let top_addr = self.top_addr();
        top_addr - (top_addr % 16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stack_allocates_requested_zeroed_memory() {
        let stack = Stack::new(256);

        assert_eq!(stack.bytes.len(), 256);
        assert!(stack.bytes.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn stack_addresses_describe_owned_memory_region() {
        let stack = Stack::new(257);
        let bottom_addr = stack.bottom_addr();
        let top_addr = stack.top_addr();
        let aligned_top = stack.aligned_top();

        assert_eq!(top_addr, bottom_addr + stack.bytes.len());
        assert!(bottom_addr < top_addr);
        assert!(bottom_addr <= aligned_top);
        assert!(aligned_top <= top_addr);
        assert_eq!(aligned_top % 16, 0);
        assert!(top_addr - aligned_top < 16);
    }
}
