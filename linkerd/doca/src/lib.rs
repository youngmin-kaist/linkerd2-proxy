use std::{
    ffi::{CStr, CString, NulError},
    fmt,
    os::raw::{c_char, c_int, c_void},
    ptr::NonNull,
};

mod driver;
mod io;

pub use driver::{
    ConnState, DmeshEvent, Driver, FlowId, Registrar, Registration, Stats, MAX_CONNS,
};
pub use io::{dmesh_io_pair, DmeshIo, DmeshIoHandle};

/// Registry of DMA backend channels: a BACKEND-mode host connection provides a
/// service at some address, and the outbound connector takes the DmeshIo from
/// here instead of dialing TCP. One channel per address; `take` hands the
/// (long-lived, h2-multiplexed) connection out once.
pub mod backend {
    use crate::DmeshIo;
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Mutex, OnceLock},
    };

    fn reg() -> &'static Mutex<HashMap<SocketAddr, DmeshIo>> {
        static R: OnceLock<Mutex<HashMap<SocketAddr, DmeshIo>>> = OnceLock::new();
        R.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn publish(addr: SocketAddr, io: DmeshIo) {
        tracing::info!(%addr, "dmesh backend channel published");
        reg().lock().unwrap().insert(addr, io);
    }

    pub fn take(addr: &SocketAddr) -> Option<DmeshIo> {
        let io = reg().lock().unwrap().remove(addr);
        if io.is_some() {
            tracing::info!(%addr, "dmesh backend channel taken by connector");
        }
        io
    }

    pub fn contains(addr: &SocketAddr) -> bool {
        reg().lock().unwrap().contains_key(addr)
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RawProbeReport {
    device_count: u32,
    devinfo_status: c_int,
    open_status: c_int,
    close_status: c_int,
    dma_status: c_int,
    aes_gcm_status: c_int,
    sha_status: c_int,
    dpa_status: c_int,
    compile_version: [c_char; 32],
    runtime_version: [c_char; 32],
    first_device_pci: [c_char; 64],
}

extern "C" {
    fn dmesh_doca_probe(report: *mut RawProbeReport) -> c_int;
    fn dmesh_doca_error_name(error: c_int) -> *const c_char;
    fn dmesh_doca_error_descr(error: c_int) -> *const c_char;
    fn dmesh_doca_init(
        dev_pci_addr: *const c_char,
        rep_pci_addr: *const c_char,
        server_name: *const c_char,
        handle: *mut *mut c_void,
    ) -> c_int;
    fn dmesh_doca_comch_destroy(handle: *mut c_void);
}

#[derive(Clone, Debug)]
pub struct ProbeReport {
    pub compile_version: String,
    pub runtime_version: String,
    pub device_count: u32,
    pub first_device_pci: String,
    pub dma: LibraryStatus,
    pub aes_gcm: LibraryStatus,
    pub sha: LibraryStatus,
    pub dpa: LibraryStatus,
}

#[derive(Clone, Debug)]
pub struct LibraryStatus {
    code: c_int,
    name: String,
    description: String,
}

#[derive(Clone, Debug)]
pub struct Error {
    code: c_int,
    name: String,
    description: String,
}

#[derive(Debug)]
// pub struct DocaComch {
pub struct DmeshDoca {
    handle: NonNull<c_void>,
}

pub fn initialize() -> Result<ProbeReport, Error> {
    let mut raw = RawProbeReport {
        device_count: 0,
        devinfo_status: 0,
        open_status: 0,
        close_status: 0,
        dma_status: 0,
        aes_gcm_status: 0,
        sha_status: 0,
        dpa_status: 0,
        compile_version: [0; 32],
        runtime_version: [0; 32],
        first_device_pci: [0; 64],
    };

    let status = unsafe { dmesh_doca_probe(&mut raw) };
    if status != 0 {
        return Err(Error::from_code(status));
    }

    Ok(ProbeReport {
        compile_version: c_array_to_string(&raw.compile_version),
        runtime_version: c_array_to_string(&raw.runtime_version),
        device_count: raw.device_count,
        first_device_pci: c_array_to_string(&raw.first_device_pci),
        dma: LibraryStatus::from_code(raw.dma_status),
        aes_gcm: LibraryStatus::from_code(raw.aes_gcm_status),
        sha: LibraryStatus::from_code(raw.sha_status),
        dpa: LibraryStatus::from_code(raw.dpa_status),
    })
}

impl DmeshDoca {
    pub fn initialize(dev_pci_addr: &str, rep_pci_addr: &str,server_name: &str,
    ) -> Result<Self, Error> {
        let dev_pci_addr = CString::new(dev_pci_addr)?;
        let rep_pci_addr = CString::new(rep_pci_addr)?;
        let server_name = CString::new(server_name)?;
        let mut handle = std::ptr::null_mut();

        let status = unsafe {
            dmesh_doca_init(
                dev_pci_addr.as_ptr(),
                rep_pci_addr.as_ptr(),
                server_name.as_ptr(),
                &mut handle,
            )
        };
        if status != 0 {
            println!("Failed to initialize DOCA comch server and datapath consumer: {}", Error::from_code(status));
            return Err(Error::from_code(status));
        }

        Ok(Self {
            handle: NonNull::new(handle).ok_or_else(|| Error::new(-1, "null DOCA comch handle"))?,
        })
    }

    pub(crate) fn raw(&self) -> *mut c_void {
        self.handle.as_ptr()
    }
}

impl Error {
    pub(crate) fn from_doca(code: c_int) -> Self {
        Self::from_code(code)
    }
}

impl ProbeReport {
    pub fn log_summary(&self) -> String {
        format!(
            "DOCA compile={} runtime={} devices={} first_device={} dma={} aes_gcm={} sha={} dpa={}",
            self.compile_version,
            self.runtime_version,
            self.device_count,
            self.first_device_pci,
            self.dma,
            self.aes_gcm,
            self.sha,
            self.dpa,
        )
    }
}

impl LibraryStatus {
    pub fn is_supported(&self) -> bool {
        self.code == 0
    }

    fn from_code(code: c_int) -> Self {
        Self {
            code,
            name: error_name(code),
            description: error_description(code),
        }
    }
}

impl Error {
    fn from_code(code: c_int) -> Self {
        Self {
            code,
            name: error_name(code),
            description: error_description(code),
        }
    }

    pub(crate) fn new(code: c_int, description: impl Into<String>) -> Self {
        Self {
            code,
            name: "invalid argument".to_string(),
            description: description.into(),
        }
    }
}

impl Drop for DmeshDoca {
    fn drop(&mut self) {
        unsafe { dmesh_doca_comch_destroy(self.handle.as_ptr()) };
    }
}

impl From<NulError> for Error {
    fn from(error: NulError) -> Self {
        Self::new(-1, error.to_string())
    }
}

impl fmt::Display for LibraryStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_supported() {
            write!(f, "ok")
        } else {
            write!(f, "{} ({})", self.name, self.description)
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}] ({})", self.name, self.code, self.description)
    }
}

impl std::error::Error for Error {}

fn error_name(code: c_int) -> String {
    unsafe { c_ptr_to_string(dmesh_doca_error_name(code)) }
}

fn error_description(code: c_int) -> String {
    unsafe { c_ptr_to_string(dmesh_doca_error_descr(code)) }
}

fn c_array_to_string<const N: usize>(array: &[c_char; N]) -> String {
    unsafe { CStr::from_ptr(array.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

unsafe fn c_ptr_to_string(ptr: *const c_char) -> String {
    if ptr.is_null() {
        return "<null>".to_string();
    }

    CStr::from_ptr(ptr).to_string_lossy().into_owned()
}
