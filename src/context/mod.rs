#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
mod aarch64_macos;

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) use aarch64_macos::Context;

#[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
compile_error!("rsroutine currently supports only Apple Silicon macOS.");
