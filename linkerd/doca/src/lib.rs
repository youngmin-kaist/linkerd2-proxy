use std::{
    ffi::CStr,
    fmt,
    os::raw::{c_char, c_int},
};

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
    fn linkerd_doca_probe(report: *mut RawProbeReport) -> c_int;
    fn linkerd_doca_error_name(error: c_int) -> *const c_char;
    fn linkerd_doca_error_descr(error: c_int) -> *const c_char;
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

    let status = unsafe { linkerd_doca_probe(&mut raw) };
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
    unsafe { c_ptr_to_string(linkerd_doca_error_name(code)) }
}

fn error_description(code: c_int) -> String {
    unsafe { c_ptr_to_string(linkerd_doca_error_descr(code)) }
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
