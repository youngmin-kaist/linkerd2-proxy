use std::{
    future::pending,
    io,
    os::fd::{AsRawFd, RawFd},
    task::{Context, Poll},
    time::Duration,
};

use tokio::io::unix::AsyncFd;
use tokio::io::Interest;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Progress {
    #[default]
    Idle,
    Pending,
    Progressed,
}

#[derive(Clone, Copy, Debug)]
pub struct NotificationFds {
    pub completion: RawFd,
    pub dma: Option<RawFd>,
    pub wake: RawFd,
}

pub trait RuntimeBackend {
    fn notification_fds(&mut self) -> io::Result<NotificationFds>;
    fn arm(&mut self) -> io::Result<()>;
    fn drain(&mut self, budget: usize) -> io::Result<Progress>;
    fn clear_notifications(&mut self) -> io::Result<()>;
    fn maintenance(&mut self) -> io::Result<()>;
    fn poll_internal(&mut self, cx: &mut Context<'_>) -> Poll<()>;
    fn stopped(&self) -> bool;
    fn ready(&mut self);
    fn failed(&mut self);
}

#[derive(Clone, Copy, Debug)]
struct BorrowedFd(RawFd);

impl AsRawFd for BorrowedFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

async fn readable(fd: Option<&AsyncFd<BorrowedFd>>) -> io::Result<()> {
    match fd {
        Some(fd) => {
            let mut guard = fd.readable().await?;
            guard.clear_ready();
            Ok(())
        }
        None => pending().await,
    }
}

fn register(fd: RawFd) -> io::Result<AsyncFd<BorrowedFd>> {
    if fd < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "negative notification fd",
        ));
    }
    AsyncFd::with_interest(BorrowedFd(fd), Interest::READABLE)
}

pub async fn run<B: RuntimeBackend>(mut backend: B) -> io::Result<()> {
    const DRAIN_BUDGET: usize = 64;
    const MAINTENANCE_PERIOD: Duration = Duration::from_millis(1);

    let result = async {
        let fds = backend.notification_fds()?;
        let completion = register(fds.completion)?;
        let dma = fds.dma.map(register).transpose()?;
        let wake = register(fds.wake)?;
        let mut next_maintenance = Instant::now();
        backend.ready();

        while !backend.stopped() {
            let now = Instant::now();
            if now >= next_maintenance {
                backend.maintenance()?;
                next_maintenance = now + MAINTENANCE_PERIOD;
            }

            if backend.drain(DRAIN_BUDGET)? == Progress::Progressed {
                tokio::task::yield_now().await;
                continue;
            }
            if backend.stopped() {
                break;
            }

            backend.arm()?;
            if backend.drain(DRAIN_BUDGET)? == Progress::Progressed {
                backend.clear_notifications()?;
                tokio::task::yield_now().await;
                continue;
            }
            if backend.stopped() {
                backend.clear_notifications()?;
                break;
            }

            tokio::select! {
                result = readable(Some(&completion)) => result?,
                result = readable(dma.as_ref()) => result?,
                result = readable(Some(&wake)) => result?,
                _ = std::future::poll_fn(|cx| backend.poll_internal(cx)) => {},
                _ = tokio::time::sleep_until(next_maintenance) => {},
            }
            backend.clear_notifications()?;
        }
        Ok(())
    }
    .await;

    if result.is_err() {
        backend.failed();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixStream;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct State {
        drains: usize,
        ready: bool,
        failed: bool,
        stopped: bool,
    }

    struct Backend {
        state: Arc<Mutex<State>>,
        completion: UnixStream,
        wake: UnixStream,
    }

    impl RuntimeBackend for Backend {
        fn notification_fds(&mut self) -> io::Result<NotificationFds> {
            Ok(NotificationFds {
                completion: self.completion.as_raw_fd(),
                dma: None,
                wake: self.wake.as_raw_fd(),
            })
        }

        fn arm(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn drain(&mut self, _budget: usize) -> io::Result<Progress> {
            let mut state = self.state.lock().unwrap();
            state.drains += 1;
            if state.drains == 1 {
                return Ok(Progress::Progressed);
            }
            state.stopped = true;
            Ok(Progress::Idle)
        }

        fn clear_notifications(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn maintenance(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn poll_internal(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }

        fn stopped(&self) -> bool {
            self.state.lock().unwrap().stopped
        }

        fn ready(&mut self) {
            self.state.lock().unwrap().ready = true;
        }

        fn failed(&mut self) {
            self.state.lock().unwrap().failed = true;
        }
    }

    fn notification_fd() -> UnixStream {
        let (reader, _writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        reader
    }

    #[tokio::test(flavor = "current_thread")]
    async fn progresses_before_waiting() {
        let state = Arc::new(Mutex::new(State::default()));
        let backend = Backend {
            state: state.clone(),
            completion: notification_fd(),
            wake: notification_fd(),
        };
        run(backend).await.unwrap();
        let state = state.lock().unwrap();
        assert!(state.ready);
        assert!(!state.failed);
        assert_eq!(state.drains, 2);
    }
}
