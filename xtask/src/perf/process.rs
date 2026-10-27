//! Bounded child-process execution and process-tree resource collection.
//!
//! The runner drains output concurrently, caps retained bytes, enforces a hard
//! watchdog, and terminates the complete child tree. Unix process groups and
//! Windows Job Objects provide platform-specific lifecycle ownership. Linux RSS
//! uses VmHWM only after the kernel clears its pre-exec flag; wait4 supplies CPU
//! and exit status. Missing RSS remains unavailable rather than including the
//! harness heap inherited at fork.

use std::{
    io::{
        self,
        Read,
    },
    path::{
        Path,
        PathBuf,
    },
    process::{
        Child,
        Command,
        ExitStatus,
        Stdio,
    },
    thread,
    time::{
        Duration,
        Instant,
    },
};

use anyhow::{
    Context,
    Result,
    bail,
};

/// Inputs for one bounded child-process execution.
pub struct ProcessSpec<'a> {
    /// Executable to launch.
    pub program: &'a Path,
    /// Command-line arguments passed without shell interpretation.
    pub args: &'a [String],
    /// Optional working directory for the child.
    pub current_dir: Option<&'a Path>,
    /// Environment variables added or overridden for the child.
    pub env: &'a [(String, String)],
    /// Hard deadline after which the entire child tree is terminated.
    pub timeout: Duration,
    /// Maximum number of stdout bytes retained in memory.
    pub stdout_limit: usize,
    /// Maximum number of stderr bytes retained in memory.
    pub stderr_limit: usize,
}

/// Exit, resource, and captured-output observations for one child process.
#[derive(Debug)]
pub struct ProcessOutcome {
    /// Elapsed wall-clock duration including process cleanup.
    pub wall_time: Duration,
    /// Process-tree CPU time when supported by the collector.
    pub cpu_time_ns: Option<u64>,
    /// Peak resident memory under the named platform collector (Linux child, Windows job).
    pub peak_rss_bytes: Option<u64>,
    /// Stable name of the platform resource collector.
    pub resource_source: &'static str,
    /// Child exit code, or `None` when no code was observable.
    pub exit_code: Option<i32>,
    /// Whether the watchdog deadline caused termination.
    pub timed_out: bool,
    /// Bounded stdout capture.
    pub stdout: CapturedOutput,
    /// Bounded stderr capture.
    pub stderr: CapturedOutput,
}

/// Bounded output retained while separately counting all observed bytes.
#[derive(Debug)]
pub struct CapturedOutput {
    /// Total bytes read from the stream, including discarded overflow.
    pub bytes_seen: u64,
    /// Whether output exceeded the configured retention limit.
    pub truncated: bool,
    /// Retained prefix of the stream.
    pub data: Vec<u8>,
    /// Time from process launch until the first observed byte.
    pub first_byte_time: Option<Duration>,
    /// Time from process launch until the most recently observed byte.
    pub last_byte_time: Option<Duration>,
}

/// Point-in-time observations for a harness-owned long-running process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessSnapshot {
    /// Whether the process had not exited when sampled.
    pub running: bool,
    /// Observed peak resident memory, with current RSS as a fallback when available.
    pub rss_bytes: Option<u64>,
    /// Current open file-descriptor count when the platform exposes it.
    pub file_descriptors: Option<u64>,
}

/// Harness-owned child process that can be observed and stopped within a bound.
pub struct RunningProcess {
    child: Child,
    group: ProcessGroup,
    started: Instant,
    stdout: Option<thread::JoinHandle<io::Result<CapturedOutput>>>,
    stderr: Option<thread::JoinHandle<io::Result<CapturedOutput>>>,
    status: Option<ExitStatus>,
    tree_terminated: bool,
}

impl CapturedOutput {
    /// Decodes the retained stream prefix with lossy UTF-8 replacement.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

/// Executes a child under platform process-tree ownership and resource collection.
pub fn execute(spec: &ProcessSpec<'_>) -> Result<ProcessOutcome> {
    RunningProcess::spawn(spec)?.wait(spec.timeout)
}

impl RunningProcess {
    /// Starts a process tree while immediately draining its bounded output streams.
    pub fn spawn(spec: &ProcessSpec<'_>) -> Result<Self> {
        let mut command = Command::new(spec.program);
        command
            .args(spec.args)
            .envs(spec.env.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(current_dir) = spec.current_dir {
            command.current_dir(current_dir);
        }
        configure_process_group(&mut command);

        let resource_baseline = capture_resource_baseline()?;
        let started = Instant::now();
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn {}", spec.program.display()))?;
        let group = ProcessGroup::attach(&mut child, resource_baseline)?;
        let stdout = drain(
            child.stdout.take().context("child stdout was not piped")?,
            spec.stdout_limit,
            started,
        );
        let stderr = drain(
            child.stderr.take().context("child stderr was not piped")?,
            spec.stderr_limit,
            started,
        );

        Ok(Self {
            child,
            group,
            started,
            stdout: Some(stdout),
            stderr: Some(stderr),
            status: None,
            tree_terminated: false,
        })
    }

    /// Samples liveness, current RSS, and descriptor count without changing lifecycle.
    pub fn snapshot(&mut self) -> Result<ProcessSnapshot> {
        self.observe()?;
        let resources = self.group.resources();
        Ok(ProcessSnapshot {
            running: self.status.is_none(),
            rss_bytes: resources.peak_rss_bytes.or_else(|| {
                self.status
                    .is_none()
                    .then(|| current_rss_bytes(self.child.id()))
                    .flatten()
            }),
            file_descriptors: open_file_descriptors(self.child.id()),
        })
    }

    /// Requests cooperative shutdown, then forcefully reaps the tree after `grace`.
    pub fn stop(mut self, grace: Duration) -> Result<ProcessOutcome> {
        self.observe()?;
        if self.status.is_none() {
            self.group.request_shutdown(&mut self.child)?;
            let deadline = Instant::now() + grace;
            while self.status.is_none() && Instant::now() < deadline {
                self.observe()?;
                thread::sleep(Duration::from_millis(10));
            }
        }
        if self.status.is_none() {
            self.terminate_tree()?;
            self.status = Some(
                self.group
                    .wait(&mut self.child)
                    .context("reap stopped child")?,
            );
        }
        self.finish(false)
    }

    fn wait(mut self, timeout: Duration) -> Result<ProcessOutcome> {
        loop {
            self.observe()?;
            if self.status.is_some() {
                return self.finish(false);
            }
            if self.started.elapsed() >= timeout {
                self.terminate_tree()?;
                self.status = Some(
                    self.group
                        .wait(&mut self.child)
                        .context("reap timed-out child")?,
                );
                return self.finish(true);
            }
            thread::sleep(Duration::from_millis(2));
        }
    }

    fn observe(&mut self) -> Result<()> {
        if self.status.is_none() {
            self.group.observe(self.child.id());
            self.status = self.group.try_wait(&mut self.child).context("poll child")?;
        }
        Ok(())
    }

    /// Closes the complete owned process tree once, including descendants left by an exited leader.
    fn terminate_tree(&mut self) -> Result<()> {
        if !self.tree_terminated {
            self.group.terminate(&mut self.child)?;
            self.tree_terminated = true;
        }
        Ok(())
    }

    fn finish(&mut self, timed_out: bool) -> Result<ProcessOutcome> {
        let status = self.status.context("cannot finish a running child")?;
        // A leader can exit while a descendant keeps an inherited output pipe open.
        // Close the owned group before joining drains so completion remains bounded.
        self.terminate_tree()?;
        let resources = self.group.resources();
        let stdout = self
            .stdout
            .take()
            .context("stdout drain was already consumed")?
            .join()
            .map_err(|_| anyhow::anyhow!("stdout drain thread panicked"))??;
        let stderr = self
            .stderr
            .take()
            .context("stderr drain was already consumed")?
            .join()
            .map_err(|_| anyhow::anyhow!("stderr drain thread panicked"))??;
        Ok(ProcessOutcome {
            wall_time: self.started.elapsed(),
            cpu_time_ns: resources.cpu_time_ns,
            peak_rss_bytes: resources.peak_rss_bytes,
            resource_source: resources.source,
            exit_code: status.code(),
            timed_out,
            stdout,
            stderr,
        })
    }
}

impl Drop for RunningProcess {
    fn drop(&mut self) {
        let _ = self.terminate_tree();
        if self.status.is_none() {
            let _ = self.group.wait(&mut self.child);
        }
        if let Some(stdout) = self.stdout.take() {
            let _ = stdout.join();
        }
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.join();
        }
    }
}

#[cfg(target_os = "linux")]
fn current_rss_bytes(process_id: u32) -> Option<u64> {
    linux_exec_completed(process_id)?;
    std::fs::read_to_string(format!("/proc/{process_id}/status"))
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
                .map(|kib| kib.saturating_mul(1024))
        })
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes(_process_id: u32) -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn open_file_descriptors(process_id: u32) -> Option<u64> {
    std::fs::read_dir(format!("/proc/{process_id}/fd"))
        .ok()
        .and_then(|entries| u64::try_from(entries.count()).ok())
}

#[cfg(not(target_os = "linux"))]
fn open_file_descriptors(_process_id: u32) -> Option<u64> {
    None
}

/// Drains one output stream without blocking the child and retains at most `limit` bytes.
fn drain<R: Read + Send + 'static>(
    mut reader: R,
    limit: usize,
    started: Instant,
) -> thread::JoinHandle<io::Result<CapturedOutput>> {
    thread::spawn(move || {
        let mut data = Vec::with_capacity(limit.min(8192));
        let mut bytes_seen = 0_u64;
        let mut first_byte_time = None;
        let mut last_byte_time = None;
        let mut buffer = [0_u8; 8192];
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let observed_at = started.elapsed();
            first_byte_time.get_or_insert(observed_at);
            last_byte_time = Some(observed_at);
            bytes_seen = bytes_seen.saturating_add(read as u64);
            let remaining = limit.saturating_sub(data.len());
            data.extend_from_slice(&buffer[..read.min(remaining)]);
        }
        Ok(CapturedOutput {
            bytes_seen,
            truncated: bytes_seen > data.len() as u64,
            data,
            first_byte_time,
            last_byte_time,
        })
    })
}

struct Resources {
    cpu_time_ns: Option<u64>,
    peak_rss_bytes: Option<u64>,
    source: &'static str,
}

#[cfg(unix)]
struct ResourceBaseline;

#[cfg(unix)]
/// Uses exact per-child `wait4` usage, so no cumulative baseline is required.
fn capture_resource_baseline() -> Result<ResourceBaseline> {
    Ok(ResourceBaseline)
}

#[cfg(not(unix))]
struct ResourceBaseline;

#[cfg(not(unix))]
/// Uses an empty baseline on platforms where the controller reports direct usage.
fn capture_resource_baseline() -> Result<ResourceBaseline> {
    Ok(ResourceBaseline)
}

#[cfg(unix)]
/// Places the Unix child in a new process group for whole-tree termination.
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

#[cfg(windows)]
/// Defers process-tree ownership to the Windows Job Object controller.
fn configure_process_group(_command: &mut Command) {}

#[cfg(not(any(unix, windows)))]
/// Leaves process grouping unchanged on unsupported platforms.
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
struct ProcessGroup {
    process_group_id: i32,
    completion_usage: Option<libc::rusage>,
    peak_rss_bytes: u64,
}

#[cfg(unix)]
impl ProcessGroup {
    fn attach(child: &mut Child, _baseline: ResourceBaseline) -> Result<Self> {
        Ok(Self {
            process_group_id: child.id() as i32,
            completion_usage: None,
            peak_rss_bytes: 0,
        })
    }

    fn observe(&mut self, process_id: u32) {
        #[cfg(target_os = "linux")]
        if let Some(peak) = linux_peak_rss_bytes(process_id) {
            self.peak_rss_bytes = self.peak_rss_bytes.max(peak);
        }
    }

    fn try_wait(&mut self, _child: &mut Child) -> Result<Option<ExitStatus>> {
        self.wait4(libc::WNOHANG)
    }

    fn wait(&mut self, _child: &mut Child) -> Result<ExitStatus> {
        self.wait4(0)?
            .context("wait4 returned no status while blocking")
    }

    fn wait4(&mut self, options: libc::c_int) -> Result<Option<ExitStatus>> {
        use std::os::unix::process::ExitStatusExt;

        loop {
            let mut status = 0;
            let mut usage = std::mem::MaybeUninit::uninit();
            let waited = unsafe {
                libc::wait4(
                    self.process_group_id,
                    &mut status,
                    options,
                    usage.as_mut_ptr(),
                )
            };
            if waited == 0 {
                return Ok(None);
            }
            if waited == self.process_group_id {
                let usage = unsafe { usage.assume_init() };
                // Linux wait4 includes the forked harness's pre-exec address space.
                // VmHWM is reset by exec, so only post-exec /proc observations
                // measure the product. A missed short process stays unmeasured.
                if !cfg!(target_os = "linux")
                    && let Some(peak_rss_bytes) = rusage_peak_rss_bytes(&usage)
                {
                    self.peak_rss_bytes = self.peak_rss_bytes.max(peak_rss_bytes);
                }
                self.completion_usage = Some(usage);
                return Ok(Some(ExitStatus::from_raw(status)));
            }

            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            bail!("wait for child process: {error}");
        }
    }

    fn terminate(&self, child: &mut Child) -> Result<()> {
        // The child is the process-group leader, so a negative PID reaches descendants too.
        let result = unsafe { libc::kill(-self.process_group_id, libc::SIGKILL) };
        if result != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(());
            }
            child.kill().context("kill timed-out child")?;
        }
        Ok(())
    }

    fn request_shutdown(&self, child: &mut Child) -> Result<()> {
        let result = unsafe { libc::kill(-self.process_group_id, libc::SIGTERM) };
        if result != 0 && child.try_wait()?.is_none() {
            bail!("signal child process group: {}", io::Error::last_os_error());
        }
        Ok(())
    }

    fn resources(&self) -> Resources {
        let cpu_time_ns = self.completion_usage.as_ref().map(|usage| {
            timeval_ns(usage.ru_utime)
                .saturating_add(timeval_ns(usage.ru_stime))
                .max(0) as u64
        });
        Resources {
            cpu_time_ns,
            peak_rss_bytes: (self.peak_rss_bytes > 0).then_some(self.peak_rss_bytes),
            source: if cfg!(target_os = "linux") {
                "linux-proc-exec-hwm-wait4-cpu-v4"
            } else {
                "unix-wait4-v2"
            },
        }
    }
}

/// Samples only the executed child's high-water mark, never its inherited harness heap.
#[cfg(target_os = "linux")]
fn linux_peak_rss_bytes(process_id: u32) -> Option<u64> {
    linux_exec_completed(process_id)?;
    let status = std::fs::read_to_string(format!("/proc/{process_id}/status")).ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmHWM:")
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|kib| kib.checked_mul(1024))
    })
}

/// A successful spawn can return while Linux still exposes the inherited address space.
/// Check the kernel's exec flag before reading memory; missing/malformed state fails closed.
#[cfg(target_os = "linux")]
fn linux_exec_completed(process_id: u32) -> Option<()> {
    let stat = std::fs::read_to_string(format!("/proc/{process_id}/stat")).ok()?;
    linux_stat_exec_completed(&stat)?.then_some(())
}

#[cfg(target_os = "linux")]
fn linux_stat_exec_completed(stat: &str) -> Option<bool> {
    // Linux include/linux/sched.h: cleared after replacing the address space in exec.
    const PF_FORKNOEXEC: u64 = 0x40;
    // Field 2 (comm) can contain spaces and parentheses. Flags are field 9.
    let (_, fields) = stat.rsplit_once(')')?;
    let flags = fields.split_whitespace().nth(6)?.parse::<u64>().ok()?;
    Some(flags & PF_FORKNOEXEC == 0)
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
fn rusage_peak_rss_bytes(usage: &libc::rusage) -> Option<u64> {
    positive_max_rss_kib_to_bytes(usage.ru_maxrss)
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios"
))]
fn positive_max_rss_kib_to_bytes(max_rss_kib: libc::c_long) -> Option<u64> {
    u64::try_from(max_rss_kib)
        .ok()
        .filter(|value| *value > 0)
        .map(|kib| kib.saturating_mul(1024))
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))
))]
fn rusage_peak_rss_bytes(_usage: &libc::rusage) -> Option<u64> {
    None
}

#[cfg(unix)]
fn timeval_ns(value: libc::timeval) -> i128 {
    i128::from(value.tv_sec)
        .saturating_mul(1_000_000_000)
        .saturating_add(i128::from(value.tv_usec).saturating_mul(1_000))
}

#[cfg(windows)]
struct ProcessGroup {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl ProcessGroup {
    fn attach(child: &mut Child, _baseline: ResourceBaseline) -> Result<Self> {
        use std::{
            ffi::c_void,
            mem::size_of,
            os::windows::io::AsRawHandle,
            ptr,
        };

        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject,
            CreateJobObjectW,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };

        let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle.is_null() {
            let _ = child.kill();
            bail!("create Windows job object: {}", io::Error::last_os_error());
        }
        let mut information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&raw const information).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        let assigned = configured != 0
            && unsafe { AssignProcessToJobObject(handle, child.as_raw_handle().cast()) } != 0;
        if !assigned {
            unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "assign child to Windows job object: {}",
                io::Error::last_os_error()
            );
        }
        Ok(Self { handle })
    }

    fn observe(&mut self, _process_id: u32) {}

    fn try_wait(&mut self, child: &mut Child) -> Result<Option<ExitStatus>> {
        child.try_wait().context("poll Windows child")
    }

    fn wait(&mut self, child: &mut Child) -> Result<ExitStatus> {
        child.wait().context("wait for Windows child")
    }

    fn terminate(&self, child: &mut Child) -> Result<()> {
        let terminated =
            unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(self.handle, 1) };
        if terminated == 0 {
            child.kill().context("kill timed-out child")?;
        }
        Ok(())
    }

    fn request_shutdown(&self, child: &mut Child) -> Result<()> {
        self.terminate(child)
    }

    fn resources(&self) -> Resources {
        use std::{
            ffi::c_void,
            mem::size_of,
            ptr,
        };

        use windows_sys::Win32::System::JobObjects::{
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectBasicAccountingInformation,
            JobObjectExtendedLimitInformation,
            QueryInformationJobObject,
        };

        let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        let accounting_ok = unsafe {
            QueryInformationJobObject(
                self.handle,
                JobObjectBasicAccountingInformation,
                (&raw mut accounting).cast::<c_void>(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                ptr::null_mut(),
            )
        } != 0;
        let mut extended = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        let extended_ok = unsafe {
            QueryInformationJobObject(
                self.handle,
                JobObjectExtendedLimitInformation,
                (&raw mut extended).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                ptr::null_mut(),
            )
        } != 0;
        Resources {
            cpu_time_ns: accounting_ok.then(|| {
                (accounting
                    .TotalUserTime
                    .saturating_add(accounting.TotalKernelTime))
                .max(0) as u64
                    * 100
            }),
            peak_rss_bytes: extended_ok.then_some(extended.PeakJobMemoryUsed as u64),
            source: "windows-job-object-v1",
        }
    }
}

#[cfg(windows)]
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
    }
}

#[cfg(not(any(unix, windows)))]
struct ProcessGroup;

#[cfg(not(any(unix, windows)))]
impl ProcessGroup {
    fn attach(_child: &mut Child, _baseline: ResourceBaseline) -> Result<Self> {
        Ok(Self)
    }

    fn observe(&mut self, _process_id: u32) {}

    fn try_wait(&mut self, child: &mut Child) -> Result<Option<ExitStatus>> {
        child.try_wait().context("poll child")
    }

    fn wait(&mut self, child: &mut Child) -> Result<ExitStatus> {
        child.wait().context("wait for child")
    }

    fn terminate(&self, child: &mut Child) -> Result<()> {
        child.kill().context("kill timed-out child")
    }

    fn request_shutdown(&self, child: &mut Child) -> Result<()> {
        self.terminate(child)
    }

    fn resources(&self) -> Resources {
        Resources {
            cpu_time_ns: None,
            peak_rss_bytes: None,
            source: "unavailable",
        }
    }
}

/// Resolves and validates a path to a regular executable file.
pub fn executable_path(path: &Path) -> Result<PathBuf> {
    if !path.is_file() {
        bail!("executable does not exist: {}", path.display());
    }
    Ok(path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    const FIXTURE_TEST: &str = "perf::process::tests::subprocess_fixture";

    fn fixture_command() -> (PathBuf, Vec<String>) {
        (
            std::env::current_exe().unwrap(),
            vec!["--exact".into(), FIXTURE_TEST.into(), "--nocapture".into()],
        )
    }

    #[test]
    fn subprocess_fixture() {
        if std::env::var_os("VIGILO_PERF_TEST_SPAWN_DESCENDANT").is_some() {
            let (program, args) = fixture_command();
            let descendant = Command::new(program)
                .args(args)
                .env_remove("VIGILO_PERF_TEST_SPAWN_DESCENDANT")
                .env("VIGILO_PERF_TEST_DELAY_MS", "5000")
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            println!("leader-exited-with-descendant={}", descendant.id());
            std::io::stdout().flush().unwrap();
            drop(descendant);
            return;
        }

        let delay_ms = std::env::var("VIGILO_PERF_TEST_DELAY_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let output_bytes = std::env::var("VIGILO_PERF_TEST_OUTPUT_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);

        let memory_bytes = std::env::var("VIGILO_PERF_TEST_MEMORY_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        assert!(
            memory_bytes <= 32 * 1024 * 1024,
            "fixture allocation must remain bounded"
        );
        let memory = vec![1_u8; memory_bytes];
        std::hint::black_box(&memory);
        std::thread::sleep(Duration::from_millis(delay_ms));
        std::hint::black_box(&memory);
        if output_bytes == 0 {
            println!("Usage: Commands:");
        } else {
            std::io::stdout()
                .write_all(&vec![b'x'; output_bytes])
                .unwrap();
            std::io::stdout().flush().unwrap();
        }
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    ))]
    #[test]
    fn rusage_peak_rss_kibibytes_are_converted_to_bytes() {
        assert_eq!(positive_max_rss_kib_to_bytes(123), Some(123 * 1024));
        assert_eq!(positive_max_rss_kib_to_bytes(0), None);
        assert_eq!(positive_max_rss_kib_to_bytes(-1), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn child_peak_rss_excludes_the_harness_heap_before_exec() {
        let padding = vec![1_u8; 192 * 1024 * 1024];
        std::hint::black_box(&padding);
        let (program, args) = fixture_command();
        let outcome = execute(&ProcessSpec {
            program: &program,
            args: &args,
            current_dir: None,
            env: &[("VIGILO_PERF_TEST_DELAY_MS".into(), "50".into())],
            timeout: Duration::from_secs(30),
            stdout_limit: 64,
            stderr_limit: 64,
        })
        .unwrap();
        std::hint::black_box(&padding);
        let peak = outcome
            .peak_rss_bytes
            .expect("observe post-exec child memory");
        assert!(
            peak < 128 * 1024 * 1024,
            "child RSS includes the parent heap: {peak}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn child_memory_is_unavailable_until_exec() {
        use std::os::{
            fd::AsRawFd,
            unix::{
                net::UnixStream,
                process::CommandExt,
            },
        };

        let (mut parent, child) = UnixStream::pair().unwrap();
        child
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        child
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        parent
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        parent
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let (program, args) = fixture_command();
        let spawning = thread::spawn(move || {
            let mut command = Command::new(program);
            command
                .args(args)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            // Only async-signal-safe syscalls and stack data are used after fork.
            // The parent releases this child before making any assertions.
            unsafe {
                command.pre_exec(move || {
                    let pid = libc::getpid().to_ne_bytes();
                    if libc::write(child.as_raw_fd(), pid.as_ptr().cast(), pid.len())
                        != pid.len() as isize
                    {
                        return Err(io::Error::last_os_error());
                    }
                    let mut release = 0_u8;
                    if libc::read(child.as_raw_fd(), (&mut release as *mut u8).cast(), 1) != 1 {
                        return Err(io::Error::from_raw_os_error(libc::EIO));
                    }
                    Ok(())
                });
            }
            command.spawn().and_then(|mut process| process.wait())
        });
        let mut pid = [0; size_of::<libc::pid_t>()];
        let received = parent.read_exact(&mut pid);
        let observed = received
            .as_ref()
            .ok()
            .map(|()| linux_peak_rss_bytes(libc::pid_t::from_ne_bytes(pid) as u32));
        let released = parent.write_all(&[1]);
        drop(parent);
        let status = spawning.join().unwrap();
        received.unwrap();
        released.unwrap();
        assert!(status.unwrap().success());
        assert_eq!(
            observed,
            Some(None),
            "pre-exec memory belongs to the harness"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn exec_state_rejects_inherited_or_unknown_memory() {
        assert_eq!(
            linux_stat_exec_completed("123 (name with ) parentheses) R 1 2 3 4 5 64 0"),
            Some(false)
        );
        assert_eq!(
            linux_stat_exec_completed("123 (name with ) parentheses) R 1 2 3 4 5 0 0"),
            Some(true)
        );
        assert_eq!(
            linux_stat_exec_completed("123 (name) R 1 2 3 4 5 4194368 0"),
            Some(false)
        );
        assert_eq!(
            linux_stat_exec_completed("123 (name) R 1 2 3 4 5 4194304 0"),
            Some(true)
        );
        for unknown in [
            "",
            "123 name R 1 2 3 4 5 0",
            "123 (name) R 1",
            "123 (name) R 1 2 3 4 5 -1",
            "123 (name) R 1 2 3 4 5 invalid",
        ] {
            assert_eq!(linux_stat_exec_completed(unknown), None);
        }
        assert_eq!(linux_peak_rss_bytes(u32::MAX), None);
        assert_eq!(current_rss_bytes(u32::MAX), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn executed_child_allocation_is_still_measured() {
        let (program, args) = fixture_command();
        let bytes = 16 * 1024 * 1024;
        let outcome = execute(&ProcessSpec {
            program: &program,
            args: &args,
            current_dir: None,
            env: &[
                ("VIGILO_PERF_TEST_MEMORY_BYTES".into(), bytes.to_string()),
                ("VIGILO_PERF_TEST_DELAY_MS".into(), "100".into()),
            ],
            timeout: Duration::from_secs(30),
            stdout_limit: 64,
            stderr_limit: 64,
        })
        .unwrap();
        assert_eq!(outcome.exit_code, Some(0));
        assert!(outcome.peak_rss_bytes.is_some_and(|peak| peak >= bytes));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn missed_post_exec_rss_is_unavailable_instead_of_inherited() {
        let (program, args) = fixture_command();
        let mut command = Command::new(program);
        command
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_process_group(&mut command);
        let mut child = command.spawn().unwrap();
        let mut group =
            ProcessGroup::attach(&mut child, capture_resource_baseline().unwrap()).unwrap();
        assert!(group.wait(&mut child).unwrap().success());
        assert_eq!(group.resources().peak_rss_bytes, None);
        assert!(group.resources().cpu_time_ns.is_some());
    }

    #[test]
    fn output_is_drained_and_bounded() {
        let (program, args) = fixture_command();
        let outcome = execute(&ProcessSpec {
            program: &program,
            args: &args,
            current_dir: None,
            env: &[("VIGILO_PERF_TEST_OUTPUT_BYTES".into(), "4096".into())],
            timeout: Duration::from_secs(30),
            stdout_limit: 64,
            stderr_limit: 64,
        })
        .unwrap();
        assert_eq!(outcome.stdout.data.len(), 64);
        assert!(outcome.stdout.bytes_seen >= 4096);
        assert!(outcome.stdout.truncated);
        assert!(outcome.stdout.first_byte_time.is_some());
        assert!(outcome.stdout.last_byte_time >= outcome.stdout.first_byte_time);
        assert!(outcome.stdout.last_byte_time <= Some(outcome.wall_time));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn short_lived_process_reports_peak_rss_after_exit() {
        let (program, args) = fixture_command();
        let outcome = execute(&ProcessSpec {
            program: &program,
            args: &args,
            current_dir: None,
            env: &[("VIGILO_PERF_TEST_DELAY_MS".into(), "50".into())],
            timeout: Duration::from_secs(30),
            stdout_limit: 64,
            stderr_limit: 64,
        })
        .unwrap();

        assert_eq!(outcome.exit_code, Some(0));
        assert!(outcome.cpu_time_ns.is_some());
        assert!(outcome.peak_rss_bytes.is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn timeout_kills_and_reaps_child() {
        let (program, args) = fixture_command();
        let outcome = execute(&ProcessSpec {
            program: &program,
            args: &args,
            current_dir: None,
            env: &[("VIGILO_PERF_TEST_DELAY_MS".into(), "10000".into())],
            timeout: Duration::from_millis(30),
            stdout_limit: 64,
            stderr_limit: 64,
        })
        .unwrap();
        assert!(outcome.timed_out);
        assert!(outcome.wall_time < Duration::from_secs(2));
    }

    #[test]
    fn leader_exit_terminates_descendant_that_retains_output_pipes() {
        let (program, args) = fixture_command();
        let watchdog = Duration::from_secs(1);
        let outcome = execute(&ProcessSpec {
            program: &program,
            args: &args,
            current_dir: None,
            env: &[("VIGILO_PERF_TEST_SPAWN_DESCENDANT".into(), "1".into())],
            timeout: watchdog,
            stdout_limit: 4096,
            stderr_limit: 4096,
        })
        .unwrap();

        assert_eq!(outcome.exit_code, Some(0));
        assert!(!outcome.timed_out);
        assert!(outcome.wall_time < watchdog);
        assert!(
            outcome
                .stdout
                .text()
                .contains("leader-exited-with-descendant=")
        );
    }

    #[test]
    fn long_running_process_can_be_sampled_and_stopped() {
        let (program, args) = fixture_command();
        let mut process = RunningProcess::spawn(&ProcessSpec {
            program: &program,
            args: &args,
            current_dir: None,
            env: &[("VIGILO_PERF_TEST_DELAY_MS".into(), "10000".into())],
            timeout: Duration::from_secs(30),
            stdout_limit: 64,
            stderr_limit: 64,
        })
        .unwrap();
        assert!(process.snapshot().unwrap().running);
        let outcome = process.stop(Duration::from_millis(50)).unwrap();
        assert!(!outcome.timed_out);
        assert!(outcome.wall_time < Duration::from_secs(2));
    }
}
