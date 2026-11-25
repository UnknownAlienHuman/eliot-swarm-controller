#![allow(
    unsafe_code,
    reason = "Authenticode verification requires this isolated WinVerifyTrust and certificate FFI boundary."
)]

use std::{
    fs::File,
    mem::size_of,
    os::windows::{ffi::OsStrExt, io::AsRawHandle},
    path::Path,
};
use windows_sys::{
    Win32::{
        Foundation::{HANDLE, INVALID_HANDLE_VALUE},
        Security::{
            Cryptography::{
                CERT_CONTEXT, CERT_NAME_ATTR_TYPE, CertGetNameStringW, szOID_COMMON_NAME,
                szOID_ORGANIZATION_NAME,
            },
            WinTrust::{
                WINTRUST_ACTION_GENERIC_VERIFY_V2, WINTRUST_DATA, WINTRUST_DATA_0,
                WINTRUST_FILE_INFO, WTD_CACHE_ONLY_URL_RETRIEVAL, WTD_CHOICE_FILE,
                WTD_REVOKE_WHOLECHAIN, WTD_STATEACTION_CLOSE, WTD_STATEACTION_VERIFY, WTD_UI_NONE,
                WTHelperGetProvCertFromChain, WTHelperGetProvSignerFromChain,
                WTHelperProvDataFromStateData, WinVerifyTrust,
            },
        },
    },
    core::PCSTR,
};

/// Verify the open file using Windows Authenticode and match its verified leaf
/// signer's exact subject attributes. Checks revocation across the whole chain
/// using cached data only, without network retrieval or UI.
pub fn authenticode_signer_matches(
    file: &File,
    path: &Path,
    common_name: &str,
    organization: &str,
) -> bool {
    let path: Vec<_> = path.as_os_str().encode_wide().chain([0]).collect();
    let mut subject = WINTRUST_FILE_INFO {
        cbStruct: size_of::<WINTRUST_FILE_INFO>() as u32,
        pcwszFilePath: path.as_ptr(),
        hFile: file.as_raw_handle(),
        ..Default::default()
    };
    let mut trust = WINTRUST_DATA {
        cbStruct: size_of::<WINTRUST_DATA>() as u32,
        dwUIChoice: WTD_UI_NONE,
        fdwRevocationChecks: WTD_REVOKE_WHOLECHAIN,
        dwUnionChoice: WTD_CHOICE_FILE,
        Anonymous: WINTRUST_DATA_0 {
            pFile: &raw mut subject,
        },
        dwStateAction: WTD_STATEACTION_VERIFY,
        dwProvFlags: WTD_CACHE_ONLY_URL_RETRIEVAL,
        ..Default::default()
    };
    let mut action = WINTRUST_ACTION_GENERIC_VERIFY_V2;
    // SAFETY: the open file, terminated path and matching union arm outlive
    // both WinVerifyTrust calls. The action and trust buffers are writable.
    let verified = unsafe {
        WinVerifyTrust(
            INVALID_HANDLE_VALUE,
            &raw mut action,
            (&raw mut trust).cast(),
        ) == 0
    };
    let matches = verified
        && !trust.hWVTStateData.is_null()
        && verified_signer_matches(trust.hWVTStateData, common_name, organization);
    if !trust.hWVTStateData.is_null() {
        trust.dwStateAction = WTD_STATEACTION_CLOSE;
        // SAFETY: this releases state from the verification call above.
        unsafe {
            WinVerifyTrust(
                INVALID_HANDLE_VALUE,
                &raw mut action,
                (&raw mut trust).cast(),
            )
        };
    }
    matches
}

fn verified_signer_matches(state: HANDLE, common_name: &str, organization: &str) -> bool {
    // SAFETY: state is live and verified until the caller closes it. The
    // helpers return borrowed trust records, which are never retained here.
    let certificate = unsafe {
        let data = WTHelperProvDataFromStateData(state);
        if data.is_null() {
            return false;
        }
        let signer = WTHelperGetProvSignerFromChain(data, 0, 0, 0);
        if signer.is_null() {
            return false;
        }
        let Some(leaf) = WTHelperGetProvCertFromChain(signer, 0).as_ref() else {
            return false;
        };
        leaf.pCert
    };
    !certificate.is_null()
        && subject_matches(certificate, szOID_COMMON_NAME, common_name)
        && subject_matches(certificate, szOID_ORGANIZATION_NAME, organization)
}

fn subject_matches(certificate: *const CERT_CONTEXT, oid: PCSTR, expected: &str) -> bool {
    let mut name = [0_u16; 128];
    // SAFETY: certificate is borrowed from the live trust state. The OID is
    // static and NUL-terminated, and the output buffer is sized in UTF-16 units.
    let length = unsafe {
        CertGetNameStringW(
            certificate,
            CERT_NAME_ATTR_TYPE,
            0,
            oid.cast(),
            name.as_mut_ptr(),
            name.len() as u32,
        )
    } as usize;
    length > 1
        && length < name.len()
        && name[..length - 1]
            .iter()
            .copied()
            .eq(expected.encode_utf16())
}
