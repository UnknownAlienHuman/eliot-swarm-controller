//! Antigravity install discovery: the `agy` CLI serves both `acp`
//! (launch) and `models` (catalogue). The Electron app path is named only
//! for guidance and is never launched, executed or version-probed.

use eliot_core as core;
use eliot_plugin::install::{
    InstallDiscovery, InstallIdentity, InstallStrategy, find_on_path, is_script_wrapper,
    local_roots,
};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Required CLI file stem: only `agy` implements the native ACP/model CLI.
pub const CLI_STEM: &str = "agy";

/// Local-roots-relative IDE subpath, named for guidance only when no CLI is
/// found. The IDE is never returned, executed or version-probed.
pub const IDE_HINT_SUBPATH: &[&str] = &["Programs", "Antigravity", "Antigravity.exe"];

/// The Antigravity install declaration read by shared install code.
pub fn discovery() -> InstallDiscovery {
    InstallDiscovery {
        name: "antigravity",
        model_hold_revision: None,
        #[cfg(windows)]
        launcher_update: None,
        exe_stems: &["agy"],
        strategy: InstallStrategy::FirstExisting,
        cli_identity: Some(CLI_STEM),
        version_identity: Some(InstallIdentity {
            matches_file: matches_cli_image,
            refusal: "executable did not establish Antigravity CLI identity and version",
        }),
        allow_script_wrapper: false,
        ide_hint_subpath: Some(IDE_HINT_SUBPATH),
        timeout_label: None,
        candidates: candidate_paths,
        searched: searched_locations,
        not_found: not_found_error,
        ide_missing: Some(ide_missing_error),
        ide_unsupported: Some(ide_unsupported_error),
        timeout_newer: None,
    }
}

fn matches_cli_image(program: &Path, deadline: Instant) -> bool {
    verify_cli_image(program, deadline).is_some()
}

/// Verify the installed console image without running it. Some Windows vendor
/// builds omit the Go module record, so authenticate the publisher there.
fn verify_cli_image(program: &Path, deadline: Instant) -> Option<()> {
    use object::{BinaryFormat, Object, ObjectSection, ReadRef, SectionFlags, read::ReadCache};
    let file = std::fs::File::open(program).ok()?;
    if file.metadata().ok()?.len() > 512 * 1024 * 1024 || Instant::now() >= deadline {
        return None;
    }
    let cache = ReadCache::new(&file);
    let image = object::File::parse(&cache).ok()?;
    #[cfg(windows)]
    if !is_windows_console(&image) {
        return None;
    }
    let section = match image.format() {
        BinaryFormat::Elf => image.section_by_name(".go.buildinfo"),
        BinaryFormat::MachO => image.section_by_name("__go_buildinfo"),
        // Go places its PE build info in the first initialized read/write section.
        BinaryFormat::Pe => image.sections().find(|section| {
            let expected = object::pe::IMAGE_SCN_CNT_INITIALIZED_DATA
                | object::pe::IMAGE_SCN_MEM_READ
                | object::pe::IMAGE_SCN_MEM_WRITE;
            matches!(section.flags(), SectionFlags::Coff { characteristics }
                if characteristics & !object::pe::IMAGE_SCN_ALIGN_32BYTES == expected)
                && section.address() != 0
                && section.size() != 0
        }),
        _ => None,
    }?;
    let (offset, size) = section.file_range()?;
    // Bound the record read even when PE places it alongside other data.
    let bytes = (&cache).read_bytes_at(offset, size.min(1024 * 1024)).ok()?;
    let header = bytes
        .as_chunks::<16>()
        .0
        .iter()
        .position(|chunk| chunk.starts_with(b"\xff Go buildinf:"))?
        * 16;
    if *bytes.get(header + 15)? & 0x02 == 0 || Instant::now() >= deadline {
        return None;
    }
    let (version, rest) = take_go_build_string(bytes.get(header + 32..)?)?;
    let (module, _) = take_go_build_string(rest)?;
    if !version.starts_with(b"go")
        || !(matches_main_module(module) || cfg!(windows) && module.is_empty())
        || Instant::now() >= deadline
    {
        return None;
    }
    #[cfg(windows)]
    if !eliot_plugin::install::authenticode_signer_matches(
        &file,
        program,
        "Google LLC",
        "Google LLC",
    ) {
        return None;
    }
    (Instant::now() < deadline).then_some(())
}

#[cfg(windows)]
fn is_windows_console<'data>(image: &object::File<'data, impl object::ReadRef<'data>>) -> bool {
    use object::read::pe::{ImageNtHeaders, ImageOptionalHeader};
    let subsystem = match image {
        object::File::Pe32(pe) => pe.nt_headers().optional_header().subsystem(),
        object::File::Pe64(pe) => pe.nt_headers().optional_header().subsystem(),
        _ => return false,
    };
    subsystem == object::pe::IMAGE_SUBSYSTEM_WINDOWS_CUI
}

fn matches_main_module(module: &[u8]) -> bool {
    // Go's cmd/go/internal/modload frames the tab-separated module records.
    let Some(module) = module
        .strip_prefix(b"\x30\x77\xaf\x0c\x92\x74\x08\x02\x41\xe1\xc1\x07\xe6\xd6\x18\xe6")
        .and_then(|record| {
            record.strip_suffix(b"\xf9\x32\x43\x31\x86\x18\x20\x72\x00\x82\x42\x10\x41\x16\xd8\xf2")
        })
    else {
        return false;
    };
    module.split(|byte| *byte == b'\n').any(|line| {
        line.strip_prefix(b"mod\t")
            .and_then(|record| record.split(|byte| *byte == b'\t').next())
            == Some(b"google-antigravity/antigravity-cli")
    })
}

fn take_go_build_string(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let mut len = 0;
    // The caller caps the whole buffer to 1 MiB, so four base-128 bytes suffice.
    for (index, &byte) in input.iter().take(4).enumerate() {
        len |= usize::from(byte & 0x7f) << (index * 7);
        if byte < 0x80 {
            return input.get(index + 1..)?.split_at_checked(len);
        }
    }
    None
}

pub(crate) fn candidate_paths() -> core::Result<Vec<PathBuf>> {
    if let Some(found) = find_on_path("agy.exe").or_else(|| find_on_path("agy")) {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(vec![found]);
        }
    }
    Ok(Vec::new())
}

fn searched_locations() -> Vec<String> {
    let mut searched: Vec<String> = vec!["agy.exe on PATH".to_owned()];
    if cfg!(not(windows)) {
        searched.push("agy on PATH".to_owned());
    }
    // An IDE-only install is reported with actionable guidance: name where
    // the IDE was seen so the message points at the fix without launching it.
    for local in local_roots() {
        let mut native = local;
        for segment in IDE_HINT_SUBPATH {
            native = native.join(segment);
        }
        searched.push(native.to_string_lossy().into_owned());
    }
    searched
}

fn not_found_error(_any_candidates: bool, searched: &[String]) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "antigravity native executable was not found; searched {}",
            searched.join(", ")
        ),
    )
}

/// Error for an `agy`-less install. The IDE path, when seen, is named for
/// guidance only: it is never returned, executed or version-probed.
fn ide_missing_error(ide: &str, searched: &[String]) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "antigravity native CLI `agy` was not found on PATH; the Antigravity IDE at {ide} does not implement the required native ACP/model CLI and is never launched for discovery. Install the `agy` CLI and put it on PATH; searched {}",
            searched.join(", ")
        ),
    )
}

/// Typed refusal for an IDE-named Antigravity cmd: named for guidance only,
/// never returned, executed or version-probed.
fn ide_unsupported_error(cmd: &str) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "antigravity cmd points at the Antigravity IDE ({cmd}), which does not implement the required native ACP/model CLI and is never launched for discovery; install the `agy` CLI and put it on PATH"
        ),
    )
}
