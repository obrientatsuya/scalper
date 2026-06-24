use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct Shutdown {
    token: CancellationToken,
}

impl Shutdown {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    pub fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}
