//! ModuleRun process creation with an explicit standard-handle allowlist.
use std::{
    cmp::Ordering,
    env,
    ffi::{OsStr, OsString},
    fs::{File, OpenOptions},
    io,
    mem::{size_of, size_of_val},
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
        process::ExitStatusExt,
    },
    process::{Command, ExitStatus},
    ptr::{null, null_mut},
};
use windows_sys::Win32::{
    Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, FALSE, HANDLE, INVALID_HANDLE_VALUE, TRUE,
        WAIT_FAILED, WAIT_OBJECT_0,
    },
    Globalization::{CSTR_EQUAL, CSTR_LESS_THAN, CompareStringOrdinal},
    Security::SECURITY_ATTRIBUTES,
    System::{
        Pipes::CreatePipe,
        Threading::{
            CREATE_NO_WINDOW, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
            DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetCurrentProcess,
            GetExitCodeProcess, InitializeProcThreadAttributeList,
            PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROCESS_INFORMATION, STARTF_USESTDHANDLES,
            STARTUPINFOEXW, UpdateProcThreadAttribute, WaitForSingleObject,
        },
    },
};

pub(super) struct Child {
    process: OwnedHandle,
    pid: u32,
    pub stdout: Option<File>,
}

impl Child {
    pub fn id(&self) -> u32 {
        self.pid
    }

    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        // SAFETY: the owned process handle remains valid for this zero-time wait.
        let result = unsafe { WaitForSingleObject(self.process.as_raw_handle().cast(), 0) };
        if result == WAIT_OBJECT_0 {
            self.status().map(Some)
        } else if result == WAIT_FAILED {
            Err(io::Error::last_os_error())
        } else {
            Ok(None)
        }
    }

    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        // SAFETY: waiting on this process never signals it or its family.
        if unsafe { WaitForSingleObject(self.process.as_raw_handle().cast(), u32::MAX) }
            != WAIT_OBJECT_0
        {
            return Err(io::Error::last_os_error());
        }
        self.status()
    }

    fn status(&self) -> io::Result<ExitStatus> {
        let mut code = 0;
        // SAFETY: the process has signaled; code is a writable exit-code slot.
        if unsafe { GetExitCodeProcess(self.process.as_raw_handle().cast(), &mut code) } == FALSE {
            return Err(io::Error::last_os_error());
        }
        Ok(ExitStatus::from_raw(code))
    }
}

pub(super) fn spawn_owner(command: &Command) -> io::Result<Child> {
    let security = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: FALSE,
    };
    let (mut read, mut write) = (null_mut(), null_mut());
    // SAFETY: output slots and security attributes remain valid for this call.
    if unsafe { CreatePipe(&mut read, &mut write, &security, 0) } == FALSE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreatePipe returned two distinct owned handles.
    let reader = unsafe { File::from_raw_handle(read.cast()) };
    let writer = unsafe { File::from_raw_handle(write.cast()) };
    let stderr = OpenOptions::new().write(true).open("NUL")?;
    let mut child = spawn(command, &writer, &stderr, CREATE_NO_WINDOW)?;
    child.stdout = Some(reader);
    Ok(child)
}

pub(super) fn spawn_bridge(command: &Command, stdout: File, stderr: File) -> io::Result<Child> {
    spawn(command, &stdout, &stderr, 0x0000_0008) // DETACHED_PROCESS
}

fn inherit(handle: HANDLE) -> io::Result<OwnedHandle> {
    let mut duplicate = null_mut();
    // SAFETY: pseudo handle is valid; the live stdio handle is duplicated into
    // a separate inheritable owned handle without changing the original flags.
    let process = unsafe { GetCurrentProcess() };
    if unsafe {
        DuplicateHandle(
            process,
            handle,
            process,
            &mut duplicate,
            0,
            TRUE,
            DUPLICATE_SAME_ACCESS,
        )
    } == FALSE
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: DuplicateHandle returned a new owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(duplicate.cast()) })
}

fn spawn(command: &Command, stdout: &File, stderr: &File, flags: u32) -> io::Result<Child> {
    let stdin_handle = std::io::stdin().as_raw_handle();
    // Match Stdio::inherit when a headless parent has no standard input.
    let stdin = if stdin_handle.is_null() || stdin_handle == INVALID_HANDLE_VALUE {
        None
    } else {
        Some(inherit(stdin_handle)?)
    };
    let stdout = inherit(stdout.as_raw_handle())?;
    let stderr = inherit(stderr.as_raw_handle())?;
    let mut raw_handles = vec![stdout.as_raw_handle(), stderr.as_raw_handle()];
    if let Some(stdin) = &stdin {
        raw_handles.push(stdin.as_raw_handle());
    }
    let attributes = Attributes::new(&raw_handles)?;
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = stdin
        .as_ref()
        .map_or(null_mut(), AsRawHandle::as_raw_handle);
    startup.StartupInfo.hStdOutput = stdout.as_raw_handle();
    startup.StartupInfo.hStdError = stderr.as_raw_handle();
    startup.lpAttributeList = attributes.list;
    let program = wide_nul(command.get_program())?;
    let mut argv = Vec::new();
    for argument in std::iter::once(command.get_program()).chain(command.get_args()) {
        if !argv.is_empty() {
            argv.push(b' ' as u16);
        }
        quote(argument, &mut argv)?;
    }
    argv.push(0);
    if argv.len() > 32767 {
        return Err(invalid("module command line exceeds the Windows limit"));
    }
    let directory = command
        .get_current_dir()
        .map(|path| wide_nul(path.as_os_str()))
        .transpose()?;
    let environment = environment(command)?;
    let mut info = PROCESS_INFORMATION::default();
    // SAFETY: strings, environment, intended owned handles and attribute storage
    // stay live through creation. Other inheritable handles are excluded.
    if unsafe {
        CreateProcessW(
            program.as_ptr(),
            argv.as_mut_ptr(),
            null(),
            null(),
            TRUE,
            flags | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT,
            environment
                .as_ref()
                .map_or(null(), |block| block.as_ptr().cast()),
            directory.as_ref().map_or(null(), |path| path.as_ptr()),
            &startup.StartupInfo,
            &mut info,
        )
    } == FALSE
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful CreateProcessW transfers two owned handles to us.
    let process = unsafe { OwnedHandle::from_raw_handle(info.hProcess.cast()) };
    let thread = unsafe { OwnedHandle::from_raw_handle(info.hThread.cast()) };
    drop(thread);
    Ok(Child {
        process,
        pid: info.dwProcessId,
        stdout: None,
    })
}

struct Attributes {
    _storage: Vec<usize>,
    list: *mut core::ffi::c_void,
}

impl Attributes {
    fn new(handles: &[HANDLE]) -> io::Result<Self> {
        let mut bytes = 0;
        // SAFETY: documented sizing call, followed by aligned owned storage.
        unsafe { InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut bytes) };
        if bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut storage = vec![0usize; bytes.div_ceil(size_of::<usize>())];
        let list = storage.as_mut_ptr().cast();
        if unsafe { InitializeProcThreadAttributeList(list, 1, 0, &mut bytes) } == FALSE {
            return Err(io::Error::last_os_error());
        }
        let attributes = Self {
            _storage: storage,
            list,
        };
        // SAFETY: the caller retains the handle list until after spawn;
        // every listed handle is a live inheritable duplicate owned by the caller.
        if unsafe {
            UpdateProcThreadAttribute(
                list,
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_ptr().cast(),
                size_of_val(handles),
                null_mut(),
                null(),
            )
        } == FALSE
        {
            return Err(io::Error::last_os_error());
        }
        Ok(attributes)
    }
}

impl Drop for Attributes {
    fn drop(&mut self) {
        // SAFETY: this list was initialized and storage is still allocated.
        unsafe { DeleteProcThreadAttributeList(self.list) };
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn wide_nul(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut result: Vec<_> = value.encode_wide().collect();
    if result.contains(&0) {
        return Err(invalid("module process argument contains NUL"));
    }
    result.push(0);
    Ok(result)
}

fn quote(value: &OsStr, output: &mut Vec<u16>) -> io::Result<()> {
    output.push(b'"' as u16);
    let mut slashes = 0;
    for unit in value.encode_wide() {
        if unit == 0 {
            return Err(invalid("module process argument contains NUL"));
        }
        if unit == b'\\' as u16 {
            slashes += 1;
        } else {
            output.extend(std::iter::repeat_n(
                b'\\' as u16,
                slashes * if unit == b'"' as u16 { 2 } else { 1 },
            ));
            if unit == b'"' as u16 {
                output.push(b'\\' as u16);
            }
            output.push(unit);
            slashes = 0;
        }
    }
    output.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2));
    output.push(b'"' as u16);
    Ok(())
}

fn ordinal(left: &OsStr, right: &OsStr) -> Ordering {
    let left: Vec<_> = left.encode_wide().collect();
    let right: Vec<_> = right.encode_wide().collect();
    // SAFETY: both buffers contain the stated UTF-16 lengths and stay alive.
    match unsafe {
        CompareStringOrdinal(
            left.as_ptr(),
            left.len() as i32,
            right.as_ptr(),
            right.len() as i32,
            TRUE,
        )
    } {
        CSTR_EQUAL => Ordering::Equal,
        CSTR_LESS_THAN => Ordering::Less,
        _ => Ordering::Greater,
    }
}

fn environment(command: &Command) -> io::Result<Option<Vec<u16>>> {
    if command.get_envs().next().is_none() {
        return Ok(None);
    }
    let mut variables: Vec<(OsString, OsString)> = env::vars_os().collect();
    for (key, value) in command.get_envs() {
        variables.retain(|(existing, _)| ordinal(existing, key) != Ordering::Equal);
        if let Some(value) = value {
            variables.push((key.to_owned(), value.to_owned()));
        }
    }
    variables.sort_by(|(left, _), (right, _)| ordinal(left, right));
    let mut block = Vec::new();
    for (key, value) in variables {
        let key: Vec<_> = key.encode_wide().collect();
        let value: Vec<_> = value.encode_wide().collect();
        if key.is_empty() || key.contains(&0) || value.contains(&0) {
            return Err(invalid(
                "module process environment contains an invalid entry",
            ));
        }
        block.extend(key);
        block.push(b'=' as u16);
        block.extend(value);
        block.push(0);
    }
    block.push(0);
    if block.len() == 1 {
        block.push(0);
    }
    Ok(Some(block))
}
