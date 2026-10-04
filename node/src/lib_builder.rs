#[cfg(unix)]
impl Drop for KillGroupOnDrop {
    fn drop(&mut self) {
        if let Some(pgid) = self.0 {
            unsafe { libc::killpg(pgid as libc::pid_t, libc::SIGKILL) };
        }
    }
}
