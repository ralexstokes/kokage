use crate::supervisor::CancellationToken;

pub(crate) struct CancelOnDrop(CancellationToken);

impl CancelOnDrop {
    pub(crate) fn new(cancellation: CancellationToken) -> Self {
        Self(cancellation)
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
