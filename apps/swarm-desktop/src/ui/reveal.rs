//! Open a path in File Explorer: a file selected in its folder, a folder
//! opened. Every row of the desktop that shows a path (chat messages and
//! tool cards, Details, Settings, Automations, Worktrees & cleanup) goes
//! through [`reveal`]. Explorer runs as `%SystemRoot%\explorer.exe` by
//! absolute path with one explicit argument and no shell; a path that is
//! not there gets a plain sentence under its row instead.

use super::*;

/// What Explorer is asked to show, as Explorer reads paths.
#[derive(Debug, PartialEq, Eq)]
enum Target {
    /// Open the folder and select the file in it.
    File(String),
    /// Open the folder itself.
    Folder(String),
}

/// Resolve `path` (a verbatim `\\?\` prefix allowed) to what Explorer shows,
/// or the plain sentence the row shows when it is not there.
fn target(path: &str) -> Result<Target, String> {
    if path.trim().is_empty() {
        return Err("No path to open.".to_owned());
    }
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        let shown = nav::display_path(path);
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("{shown} is not on this computer (it was moved or deleted).")
        } else {
            format!("{shown} could not be opened: {error}")
        }
    })?;
    let shown = explorer_path(&canonical)?;
    Ok(if canonical.is_dir() {
        Target::Folder(shown)
    } else {
        Target::File(shown)
    })
}

/// Explorer receives only an ordinary absolute drive or UNC path whose
/// Win32 normalization round-trips to the exact canonical target.
fn explorer_path(canonical: &Path) -> Result<String, String> {
    use std::path::{Component, Prefix};
    let unrepresentable = || {
        format!(
            "File Explorer cannot open this path without changing its target: {}",
            canonical.display()
        )
    };
    let path = canonical.to_str().ok_or_else(unrepresentable)?;
    let shown = match path.strip_prefix(r"\\?\UNC\") {
        Some(rest) => format!(r"\\{rest}"),
        None => path.strip_prefix(r"\\?\").unwrap_or(path).to_owned(),
    };
    let ordinary = Path::new(&shown);
    let mut components = ordinary.components();
    if !matches!(
        components.next(),
        Some(Component::Prefix(prefix)) if matches!(prefix.kind(), Prefix::Disk(_) | Prefix::UNC(_, _))
    ) || !ordinary.is_absolute()
        || shown
            .split('\\')
            .skip(1)
            .any(|part| part.ends_with(['.', ' ']))
        || !std::fs::canonicalize(ordinary)
            .is_ok_and(|round_trip| round_trip.as_os_str() == canonical.as_os_str())
    {
        return Err(unrepresentable());
    }
    Ok(shown)
}

/// Explorer's own command line. It does not split argv the C runtime way:
/// the `/select,` switch stays outside the quoted path, and a quoted folder
/// keeps a comma in its name from reading as a switch separator. Windows
/// file names cannot contain a quote, so quoting never needs escapes. A
/// drive root keeps its trailing backslash out of the quotes' way.
fn explorer_arg(target: &Target) -> String {
    match target {
        Target::File(path) => format!("/select,\"{path}\""),
        Target::Folder(path) if path.ends_with('\\') => format!("\"{path}.\""),
        Target::Folder(path) => format!("\"{path}\""),
    }
}

/// `%SystemRoot%\explorer.exe`, by absolute path.
#[expect(
    clippy::disallowed_methods,
    reason = "This helper builds the platform file-reveal command."
)]
#[expect(
    clippy::disallowed_types,
    reason = "This helper returns the File Explorer command used by file reveal."
)]
fn explorer() -> Result<std::process::Command, String> {
    let root = std::env::var_os("SystemRoot")
        .ok_or_else(|| "File Explorer was not found: SystemRoot is not set.".to_owned())?;
    Ok(std::process::Command::new(explorer_executable(Path::new(
        &root,
    ))?))
}

fn explorer_executable(root: &Path) -> Result<PathBuf, String> {
    if !root.is_absolute() {
        return Err("File Explorer was not found: SystemRoot is not an absolute path.".to_owned());
    }
    Ok(root.join("explorer.exe"))
}

/// Show `path` in File Explorer: a file selected in its folder, a folder
/// opened. Nothing is waited for. `Err` is the sentence to show the owner.
pub(super) fn reveal(path: &str) -> Result<(), String> {
    use std::os::windows::process::CommandExt;
    let target = target(path)?;
    explorer()?
        .raw_arg(explorer_arg(&target))
        .spawn()
        .map(drop)
        .map_err(|error| format!("File Explorer did not start: {error}"))
}

/// Open a file with its default app, as a double click in Explorer does
/// (Open config.toml). Nothing is waited for.
pub(super) fn open_with_default_app(path: &Path) -> Result<(), String> {
    let path = match target(&path.to_string_lossy())? {
        Target::File(path) | Target::Folder(path) => path,
    };
    explorer()?
        .arg(path)
        .spawn()
        .map(drop)
        .map_err(|error| format!("File Explorer did not start: {error}"))
}

/// `relative` under `worktree` as an existing canonical path that stays
/// inside the worktree's canonical root (a link or junction that leads
/// out is refused). Runs file system calls: call it off the UI thread.
pub(super) fn inside_worktree(worktree: &Path, relative: &Path) -> Result<PathBuf, String> {
    let root = std::fs::canonicalize(worktree)
        .map_err(|error| format!("The worktree could not be read: {error}"))?;
    let full = std::fs::canonicalize(worktree.join(relative)).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!(
                "{} is not on this computer (it was moved or deleted).",
                relative.display()
            )
        } else {
            format!("{} could not be opened: {error}", relative.display())
        }
    })?;
    if full.starts_with(&root) {
        Ok(full)
    } else {
        Err(format!(
            "{} leads outside the worktree.",
            relative.display()
        ))
    }
}

/// Open a changed file to read or edit it, never to run it: a program, a
/// script, a link or an installer is refused, since its default action
/// would execute it. That is an extension in `PATHEXT` or in [`RUNS`], or
/// one whose shell "open" verb starts a script host or interpreter
/// ([`INTERPRETERS`], read from the machine's own association). Any other
/// file opens with its default app. It reads the shell association: call
/// it off the UI thread. Nothing is waited for.
pub(super) fn open_for_editing(path: &Path) -> Result<(), String> {
    let extension = path
        .extension()
        .map(|extension| extension.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default();
    let path_ext = std::env::var("PATHEXT")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let runs = !extension.is_empty()
        && (RUNS.contains(&extension.as_str())
            || path_ext
                .split(';')
                .any(|listed| listed.trim_start_matches('.') == extension)
            || open_program(&extension).is_some_and(|program| {
                // `%1`: the file itself is what runs (a batch file's own verb).
                program == "%1" || INTERPRETERS.contains(&program.as_str())
            }));
    if runs {
        return Err(format!(
            "{} would run if opened, so it is not opened. Use Open folder to see it.",
            path.file_name().map_or_else(
                || path.display().to_string(),
                |name| name.to_string_lossy().into_owned()
            )
        ));
    }
    open_with_default_app(path)
}

/// Extensions whose default action runs code, beyond `PATHEXT`.
const RUNS: &[&str] = &[
    "exe",
    "com",
    "bat",
    "cmd",
    "ps1",
    "psm1",
    "psd1",
    "ps1xml",
    "psc1",
    "vbs",
    "vbe",
    "js",
    "jse",
    "wsf",
    "wsh",
    "ws",
    "wsc",
    "msc",
    "msi",
    "msp",
    "mst",
    "scr",
    "pif",
    "lnk",
    "url",
    "hta",
    "cpl",
    "reg",
    "jar",
    "appref-ms",
    "application",
    "gadget",
    "inf",
    "scf",
    "chm",
    "dll",
    "sys",
    "appx",
    "appxbundle",
    "msix",
    "msixbundle",
    "library-ms",
    "search-ms",
    "settingcontent-ms",
    "website",
    "py",
    "pyw",
    "pyz",
    "rb",
    "pl",
    "sh",
    "bash",
    "zsh",
    "ksh",
    "csh",
    "tcsh",
    "fish",
    "command",
];

/// Programs that run the file they open: a shell association whose
/// default "open" starts one of these makes the extension a script.
const INTERPRETERS: &[&str] = &[
    "bash.exe",
    "sh.exe",
    "zsh.exe",
    "git-bash.exe",
    "mintty.exe",
    "wsl.exe",
    "wscript.exe",
    "cscript.exe",
    "powershell.exe",
    "powershell_ise.exe",
    "pwsh.exe",
    "cmd.exe",
    "mshta.exe",
    "rundll32.exe",
    "msiexec.exe",
    "regedit.exe",
    "python.exe",
    "pythonw.exe",
    "py.exe",
    "pyw.exe",
    "node.exe",
    "java.exe",
    "javaw.exe",
    "perl.exe",
    "ruby.exe",
    "php.exe",
];

/// The program file name (lowercase) that the shell's default "open"
/// verb starts for `.extension` on this machine, when an association names
/// one.
#[allow(
    unsafe_code,
    reason = "The shell association is read through shlwapi's AssocQueryStringW."
)]
fn open_program(extension: &str) -> Option<String> {
    use windows_sys::Win32::UI::Shell::{
        ASSOCF_INIT_IGNOREUNKNOWN, ASSOCF_NOTRUNCATE, ASSOCSTR_EXECUTABLE, AssocQueryStringW,
    };
    let wide = |text: &str| text.encode_utf16().chain(Some(0)).collect::<Vec<u16>>();
    let (assoc, verb) = (wide(&format!(".{extension}")), wide("open"));
    let mut buffer = vec![0u16; 1024];
    let mut length = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
    // SAFETY: both inputs are NUL-terminated; the buffer and its length
    // in characters match, and the call writes at most that many.
    let result = unsafe {
        AssocQueryStringW(
            ASSOCF_INIT_IGNOREUNKNOWN | ASSOCF_NOTRUNCATE,
            ASSOCSTR_EXECUTABLE,
            assoc.as_ptr(),
            verb.as_ptr(),
            buffer.as_mut_ptr(),
            &mut length,
        )
    };
    if result != 0 {
        return None;
    }
    let end = buffer
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(buffer.len());
    let program = String::from_utf16_lossy(&buffer[..end]);
    Path::new(&program)
        .file_name()
        .map(|name| name.to_string_lossy().to_ascii_lowercase())
}

/// Open `folder` in Windows Terminal (`wt.exe -d <folder>`): the app
/// execution alias under `%LOCALAPPDATA%\Microsoft\WindowsApps`, by
/// absolute path with explicit argv and no shell. The terminal is the
/// owner's: it keeps the owner's environment (Windows Terminal does not load
/// its settings without it) except Swarm's own variables, so no Eliot
/// identity or credential from this window reaches it; only `ELIOT_HOME`,
/// which names the data directory, stays. Nothing is waited for. `Err` is
/// the sentence to show the owner.
pub(super) fn open_terminal(folder: &str) -> Result<(), String> {
    let Target::Folder(path) = target(folder)? else {
        return Err(format!("{} is not a folder.", nav::display_path(folder)));
    };
    // Windows Terminal splits its command line at `;` into new tabs.
    if path.contains(';') {
        return Err(
            "Windows Terminal reads a ; in the folder's path as a new command: open the folder from File Explorer instead."
                .to_owned(),
        );
    }
    let local = std::env::var_os("LOCALAPPDATA")
        .ok_or_else(|| "Windows Terminal was not found: LOCALAPPDATA is not set.".to_owned())?;
    let wt = Path::new(&local)
        .join("Microsoft")
        .join("WindowsApps")
        .join("wt.exe");
    if !wt.is_absolute() || std::fs::symlink_metadata(&wt).is_err() {
        return Err("Windows Terminal is not installed (wt.exe was not found).".to_owned());
    }
    terminal_command(&wt)
        .arg("-d")
        .arg(&path)
        .spawn()
        .map(drop)
        .map_err(|error| format!("Windows Terminal did not start: {error}"))
}

/// Windows Terminal's command without Swarm's variables (but `ELIOT_HOME`).
#[expect(
    clippy::disallowed_types,
    reason = "This helper returns the Windows Terminal command Open in Windows Terminal starts."
)]
fn terminal_command(wt: &Path) -> std::process::Command {
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs the Windows Terminal process owned by this module."
    )]
    let mut command = std::process::Command::new(wt);
    for (name, _) in std::env::vars_os() {
        let upper = name.to_string_lossy().to_ascii_uppercase();
        if upper.starts_with("ELIOT_") && upper != "ELIOT_HOME" {
            command.env_remove(&name);
        }
    }
    command
}

const TOOLTIP: &str = "Open in Explorer";

/// A full-width row for a path: its title, a note under it (the path when
/// empty) and the open glyph at the end; hover and press states and
/// Enter/Space come from the row button, and the tooltip says what a click
/// does.
pub(super) fn path_row(title: &str, note: &str, path: &str, open: Callback<()>) -> View {
    let shown = nav::display_path(path);
    let note = if note.is_empty() { &shown } else { note };
    controls::two_line_row(false)
        .automation_name(format!("Open {title} in Explorer: {shown}"))
        .on_click(open)
        .content(
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::SM)
                .children((
                    StackPanel::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .children((
                            controls::body(title.to_owned())
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            controls::secondary(note.to_owned())
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                    Border::new()
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(controls::icon(controls::glyph::OPEN_FOLDER)),
                )),
        )
        .tooltip(format!("{TOOLTIP}: {note}"))
}

/// A list item about a path whose lines wrap (a cleanup item): the whole
/// item opens the path, as [`path_row`] does, at the height of its lines.
pub(super) fn path_item(name: &str, content: View, open: Callback<()>) -> View {
    controls::row(false)
        .automation_name(format!("Open {name} in Explorer"))
        .on_click(open)
        .content(
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::SM)
                .children((
                    content,
                    Border::new()
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(controls::icon(controls::glyph::OPEN_FOLDER)),
                )),
        )
        .tooltip(TOOLTIP)
}

/// A path inside a sentence's block (Automations, Details): the path in
/// the link color with WinUI's own hover state; the label keeps the
/// text's inset, so it lines up with the lines around it.
pub(super) fn path_link(path: &str, open: Callback<()>) -> View {
    let shown = nav::display_path(path);
    controls::text_button()
        .style(ButtonStyle::Subtle)
        .min_height(tokens::height::SUB_ROW)
        .horizontal_alignment(HorizontalAlignment::Left)
        .margin(Thickness::new(-tokens::spacing::XS, 0.0, 0.0, 0.0))
        .automation_name(format!("Open in Explorer: {shown}"))
        .on_click(open)
        // A star cell gives the path the room left beside its icon, so it
        // trims with an ellipsis; a horizontal stack let it run past the
        // button's edge and cut it off.
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(tokens::spacing::XS)
                .margin(Thickness::xy(tokens::spacing::XS, 0.0))
                .children((
                    controls::icon(controls::glyph::OPEN_FOLDER),
                    controls::text(shown.to_owned(), tokens::text::BODY)
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                )),
        )
        .tooltip(format!("{TOOLTIP}: {shown}"))
}

/// A square open-in-Explorer button beside a path field (Settings ›
/// Projects' checkout folder).
pub(super) fn path_button(name: &str, open: Callback<()>) -> View {
    controls::icon_button()
        .automation_name(format!("Open {name} in Explorer"))
        .on_click(open)
        .content(controls::icon(controls::glyph::OPEN_FOLDER))
        .tooltip(TOOLTIP)
}

impl DesktopApp {
    /// Open `path` in Explorer; a failure stays under the path's row until
    /// the next open.
    pub(super) fn reveal_path(&mut self, item: String, path: String) {
        self.revealed = reveal(&path).err().map(|note| (item, note));
    }

    /// The message a path click sends.
    pub(super) fn reveal_message(
        &self,
        context: &mut ViewContext<Self>,
        path: &str,
    ) -> Callback<()> {
        self.reveal_item_message(context, path, path)
    }

    pub(super) fn reveal_item_message(
        &self,
        context: &mut ViewContext<Self>,
        item: &str,
        path: &str,
    ) -> Callback<()> {
        context.message(Message::Reveal(item.to_owned(), path.to_owned()))
    }

    /// Why the last open of `path` failed, in the error color; empty
    /// otherwise.
    pub(super) fn reveal_note(&self, item: &str) -> Option<View> {
        match &self.revealed {
            Some((failed, note)) if failed == item => Some(
                controls::secondary(note.clone())
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .into(),
            ),
            _ => None,
        }
    }

    /// [`path_row`] with its failure note under it.
    pub(super) fn path_row(
        &self,
        context: &mut ViewContext<Self>,
        title: &str,
        note: &str,
        path: &str,
    ) -> View {
        let open = self.reveal_message(context, path);
        StackPanel::new()
            .keyed_children(crate::views::slots([
                Some(path_row(title, note, path, open)),
                self.reveal_note(path),
            ]))
            .into()
    }

    /// [`path_link`] with its failure note under it.
    pub(super) fn path_link(&self, context: &mut ViewContext<Self>, path: &str) -> View {
        let open = self.reveal_message(context, path);
        StackPanel::new()
            .keyed_children(crate::views::slots([
                Some(path_link(path, open)),
                self.reveal_note(path),
            ]))
            .into()
    }
}
