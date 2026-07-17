//! P1 smoke test: bring the DMesh driver up on the DPU without a host peer.
//!
//! Verifies, on real hardware, that the Rust AsyncFd driver can (1) start the
//! comch server, (2) drive the shared infrastructure (DPA pool, consumer PE,
//! ...) to RUNNING via the C state machine, and (3) idle event-driven on the
//! PE notification fds. If a host worker connects while this runs, connection
//! events are printed too.
//!
//! Usage (on the DPU):
//!   DMESH_DEV_PCI=03:00.0 DMESH_REP_PCI=94:00.0 \
//!     cargo run -p dmesh-doca --example p1_smoke

use std::time::Duration;

use dmesh_doca::{DmeshDoca, DmeshEvent, Driver};
use tokio::sync::mpsc;

fn main() {
    let dev = std::env::var("DMESH_DEV_PCI").unwrap_or_else(|_| "03:00.0".to_string());
    let rep = std::env::var("DMESH_REP_PCI").unwrap_or_else(|_| "94:00.0".to_string());
    let name = std::env::var("DMESH_SERVER").unwrap_or_else(|_| "DMeshP1".to_string());
    let secs: u64 = std::env::var("DMESH_SMOKE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    println!("[p1_smoke] dev={dev} rep={rep} server={name}");

    let doca = match DmeshDoca::initialize(&dev, &rep, &name) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[p1_smoke] FAIL: DOCA init: {e}");
            std::process::exit(1);
        }
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async move {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let driver = Driver::new(doca, tx);
        let mut driver_task = tokio::spawn(driver.run());

        let deadline = tokio::time::sleep(Duration::from_secs(secs));
        tokio::pin!(deadline);

        let mut infra_ready = false;
        loop {
            tokio::select! {
                ev = rx.recv() => match ev {
                    Some(DmeshEvent::InfraReady) => {
                        infra_ready = true;
                        println!("[p1_smoke] infra RUNNING (DPA pool + consumer PE up)");
                    }
                    Some(ev) => println!("[p1_smoke] event: {ev:?}"),
                    None => break,
                },
                res = &mut driver_task => {
                    eprintln!("[p1_smoke] FAIL: driver exited early: {res:?}");
                    std::process::exit(1);
                }
                _ = &mut deadline => break,
            }
        }

        if infra_ready {
            println!("[p1_smoke] OK: driver idled event-driven for {secs}s");
        } else {
            eprintln!("[p1_smoke] FAIL: infra did not reach RUNNING within {secs}s");
            std::process::exit(1);
        }
    });
}
