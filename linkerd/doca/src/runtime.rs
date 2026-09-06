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

/// Notification sources a pass saw readable. Only those are cleared: clearing
/// a source that did not fire is a read that returns nothing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Fired {
    pub completion: bool,
    pub dma: bool,
    pub wake: bool,
}

pub trait RuntimeBackend {
    fn notification_fds(&mut self) -> io::Result<NotificationFds>;
    fn arm(&mut self) -> io::Result<()>;
    fn drain(&mut self, budget: usize) -> io::Result<Progress>;
    fn clear_notifications(&mut self, fired: Fired) -> io::Result<()>;
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

/// Whether an edge arrived on `fd` since it was last cleared, consuming it.
/// A source that becomes readable right after this answer is still reported
/// by the next wait: readiness is sticky until cleared.
fn seen(fd: Option<&AsyncFd<BorrowedFd>>) -> bool {
    let mut cx = Context::from_waker(std::task::Waker::noop());
    match fd.map(|fd| fd.poll_read_ready(&mut cx)) {
        Some(Poll::Ready(Ok(mut guard))) => {
            guard.clear_ready();
            true
        }
        _ => false,
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
        // Whether the internal poll may be waited on this pass.
        //
        // `poll_internal` answers whether the backend holds work, not whether
        // anything has changed since it was last asked. A pass that waits on it
        // and then publishes nothing would find the same answer immediately, so
        // waiting on it again would spin the thread and never return it to the
        // scheduler that drives every other task on this runtime. One such wait
        // takes it down until a notification, a drain that progresses or the
        // maintenance deadline says something moved. Nothing is dropped: every
        // pass drains before it waits.
        let mut internal_armed = true;
        // One timer entry serves every wait; registering a timer wakes the
        // runtime's driver, so the entry is only reset once per period.
        let mut maintenance = std::pin::pin!(tokio::time::sleep_until(next_maintenance));
        // The sources whose edges were consumed by the wait just completed.
        let seen_all = |completion: &AsyncFd<BorrowedFd>,
                        dma: Option<&AsyncFd<BorrowedFd>>,
                        wake: &AsyncFd<BorrowedFd>,
                        mut fired: Fired| {
            fired.completion |= seen(Some(completion));
            fired.dma |= seen(dma);
            fired.wake |= seen(Some(wake));
            fired
        };
        backend.ready();

        while !backend.stopped() {
            let now = Instant::now();
            if now >= next_maintenance {
                backend.maintenance()?;
                next_maintenance = now + MAINTENANCE_PERIOD;
                maintenance.as_mut().reset(next_maintenance);
            }

            if backend.drain(DRAIN_BUDGET)? == Progress::Progressed {
                internal_armed = true;
                tokio::task::yield_now().await;
                continue;
            }
            if backend.stopped() {
                break;
            }

            backend.arm()?;
            if backend.drain(DRAIN_BUDGET)? == Progress::Progressed {
                internal_armed = true;
                backend.clear_notifications(seen_all(
                    &completion,
                    dma.as_ref(),
                    &wake,
                    Fired::default(),
                ))?;
                tokio::task::yield_now().await;
                continue;
            }
            if backend.stopped() {
                backend.clear_notifications(seen_all(
                    &completion,
                    dma.as_ref(),
                    &wake,
                    Fired::default(),
                ))?;
                break;
            }

            let mut fired = Fired::default();
            tokio::select! {
                result = readable(Some(&completion)) => {
                    internal_armed = true;
                    fired.completion = true;
                    result?
                },
                result = readable(dma.as_ref()) => {
                    internal_armed = true;
                    fired.dma = true;
                    result?
                },
                result = readable(Some(&wake)) => {
                    internal_armed = true;
                    fired.wake = true;
                    result?
                },
                _ = std::future::poll_fn(|cx| backend.poll_internal(cx)), if internal_armed => {
                    internal_armed = false;
                },
                _ = maintenance.as_mut() => {
                    internal_armed = true;
                },
            }
            backend.clear_notifications(seen_all(&completion, dma.as_ref(), &wake, fired))?;
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

        fn clear_notifications(&mut self, _fired: Fired) -> io::Result<()> {
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

    /// A notification fd and the peer that keeps it from reading as readable.
    fn quiet_notification_fd() -> (UnixStream, UnixStream) {
        let (reader, writer) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        (reader, writer)
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

    #[derive(Default)]
    struct StuckState {
        drains: usize,
        internal_polls: usize,
        maintenances: usize,
        stopped: bool,
    }

    /// A backend that reports internal work on every ask and never publishes
    /// any of it: the shape a session another endpoint keeps open leaves
    /// behind. Waiting on that answer must not be what the next pass does.
    ///
    /// Both peers are held, because a notification fd whose writer is gone
    /// reads as readable and the pass would wake on that instead.
    struct StuckBackend {
        state: Arc<Mutex<StuckState>>,
        completion: UnixStream,
        wake: UnixStream,
        _peers: (UnixStream, UnixStream),
    }

    /// Passes to observe. Two drains make a pass, so this is 20 of them.
    const STUCK_DRAINS: usize = 40;

    impl RuntimeBackend for StuckBackend {
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
            if state.drains >= STUCK_DRAINS {
                state.stopped = true;
            }
            Ok(Progress::Idle)
        }

        fn clear_notifications(&mut self, _fired: Fired) -> io::Result<()> {
            Ok(())
        }

        fn maintenance(&mut self) -> io::Result<()> {
            self.state.lock().unwrap().maintenances += 1;
            Ok(())
        }

        fn poll_internal(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
            self.state.lock().unwrap().internal_polls += 1;
            Poll::Ready(())
        }

        fn stopped(&self) -> bool {
            self.state.lock().unwrap().stopped
        }

        fn ready(&mut self) {}

        fn failed(&mut self) {
            self.state.lock().unwrap().stopped = true;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unpublishable_internal_work_does_not_spin() {
        let state = Arc::new(Mutex::new(StuckState::default()));
        let (completion, completion_peer) = quiet_notification_fd();
        let (wake, wake_peer) = quiet_notification_fd();
        let backend = StuckBackend {
            state: state.clone(),
            completion,
            wake,
            _peers: (completion_peer, wake_peer),
        };
        run(backend).await.unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.drains, STUCK_DRAINS);
        // One wait on the internal poll per two passes at most: the pass after
        // it waits on the notifications and the maintenance deadline instead.
        assert!(
            state.internal_polls <= STUCK_DRAINS / 4 + 1,
            "internal polls {} over {} drains",
            state.internal_polls,
            state.drains,
        );
        // Reaching the deadline is what a pass that waits does, and a pass that
        // does not wait never reaches it.
        assert!(
            state.maintenances > 1,
            "maintenances {}",
            state.maintenances
        );
    }
}
