use futures::{channel::mpsc, lock::Mutex, stream::FusedStream, StreamExt};

/// Notifies a single task to wake up.
///
/// If `notify_one()` is called before `notified().await`, then the next call to
/// `notified().await` will complete immediately, consuming the permit. Any
/// subsequent calls to `notified().await` will wait for a new permit.
///
/// If `notify_one()` is called multiple times before `notified().await`, only a
/// single permit is stored. The next call to `notified().await` will complete
/// immediately, but the one after will wait for a new permit.
pub(crate) struct Notify {
    sender: mpsc::Sender<()>,
    receiver: Mutex<mpsc::Receiver<()>>,
}

impl Notify {
    /// Create a new Notify, initialized without a permit.
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        Notify {
            sender,
            receiver: Mutex::new(receiver),
        }
    }

    /// Wait for a notification.
    pub(crate) async fn notified(&self) {
        let mut receiver = self.receiver.lock().await;
        if !receiver.is_terminated() {
            receiver.next().await.unwrap_or_default();
        }
    }

    /// Notifies a waiting task.
    pub(crate) fn notify_one(&self) {
        // Ignore if there has already exists a permit.
        self.sender.clone().try_send(()).unwrap_or_default();
    }

    /// Notifies all waiting or incoming task.
    pub(crate) fn notify_all(&self) {
        self.sender.clone().close_channel();
    }
}

impl Default for Notify {
    fn default() -> Self {
        Notify::new()
    }
}
