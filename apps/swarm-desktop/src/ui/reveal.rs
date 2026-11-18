//! Open a path in File Explorer: a file selected in its folder, a folder
//! opened. Every row of the desktop that shows a path (chat messages and
//! tool cards, Details, Settings, Automations, Worktrees & cleanup) goes
//! through [`reveal`]. Explorer runs as `%SystemRoot%\explorer.exe` by
//! absolute path with one explicit argument and no shell; a path that is
//! not there gets a plain sentence under its row instead.

use super::*;
use crate::theme::winui::Palette;

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
    let path = path.trim();
    if path.is_empty() {
        return Err("No path to open.".to_owned());
    }
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        let shown = explorer_path(path);
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("{shown} is not on this computer (it was moved or deleted).")
        } else {
            format!("{shown} could not be opened: {error}")
        }
    })?;
    let shown = explorer_path(&canonical.to_string_lossy());
    Ok(if canonical.is_dir() {
        Target::Folder(shown)
    } else {
        Target::File(shown)
    })
}

/// A path as Explorer accepts it: Explorer does not read the verbatim
/// prefix that `canonicalize` adds (`\\?\C:\x` is `C:\x`, `\\?\UNC\host\
/// share` is `\\host\share`).
fn explorer_path(path: &str) -> String {
    match path.strip_prefix(r"\\?\UNC\") {
        Some(rest) => format!(r"\\{rest}"),
        None => path.strip_prefix(r"\\?\").unwrap_or(path).to_owned(),
    }
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
fn explorer() -> Result<std::process::Command, String> {
    let root = std::env::var_os("SystemRoot")
        .ok_or_else(|| "File Explorer was not found: SystemRoot is not set.".to_owned())?;
    Ok(std::process::Command::new(
        PathBuf::from(root).join("explorer.exe"),
    ))
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
    explorer()?
        .arg(explorer_path(&path.to_string_lossy()))
        .spawn()
        .map(drop)
        .map_err(|error| format!("File Explorer did not start: {error}"))
}

const TOOLTIP: &str = "Open in Explorer";

/// A full-width row for a path: its title, a note under it (the path when
/// empty) and the open glyph at the end; hover and press states and
/// Enter/Space come from the row button, and the tooltip says what a click
/// does.
pub(super) fn path_row(
    palette: Palette,
    title: &str,
    note: &str,
    path: &str,
    open: Callback<()>,
) -> View {
    let shown = nav::display_path(path);
    let note = if note.is_empty() { shown } else { note };
    m3::row(palette, false)
        .height(tokens::height::TWO_LINE_ROW)
        .min_height(tokens::height::TWO_LINE_ROW)
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
                            m3::body(title.to_owned())
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            m3::secondary(note.to_owned())
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                    Border::new()
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(m3::icon(m3::glyph::OPEN_FOLDER)),
                )),
        )
        .tooltip(TOOLTIP)
}

/// A list item about a path whose lines wrap (a cleanup item): the whole
/// item opens the path, as [`path_row`] does, at the height of its lines.
pub(super) fn path_item(palette: Palette, name: &str, content: View, open: Callback<()>) -> View {
    Button::new()
        .resource_overrides(palette.row(false))
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
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
                        .content(m3::icon(m3::glyph::OPEN_FOLDER)),
                )),
        )
        .tooltip(TOOLTIP)
}

/// A path inside a sentence's block (Automations, Details): the path in
/// the link color with a state layer that hugs it; the label keeps the
/// text's inset, so it lines up with the lines around it.
pub(super) fn path_link(palette: Palette, path: &str, open: Callback<()>) -> View {
    let shown = nav::display_path(path);
    m3::text_button(palette)
        .style(ButtonStyle::Subtle)
        .min_height(0.0)
        .height(tokens::height::SUB_ROW)
        .horizontal_alignment(HorizontalAlignment::Left)
        .margin(Thickness::new(-tokens::spacing::XS, 0.0, 0.0, 0.0))
        .automation_name(format!("Open in Explorer: {shown}"))
        .on_click(open)
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::XS)
                .margin(Thickness::xy(tokens::spacing::XS, 0.0))
                .children((
                    m3::icon(m3::glyph::OPEN_FOLDER),
                    TextBlock::new()
                        .text(shown.to_owned())
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                )),
        )
        .tooltip(TOOLTIP)
}

/// A square open-in-Explorer button beside a path field (Settings ›
/// Projects' checkout folder).
pub(super) fn path_button(palette: Palette, name: &str, open: Callback<()>) -> View {
    m3::icon_button(palette)
        .automation_name(format!("Open {name} in Explorer"))
        .on_click(open)
        .content(m3::icon(m3::glyph::OPEN_FOLDER))
        .tooltip(TOOLTIP)
}

impl DesktopApp {
    /// Open `path` in Explorer; a failure stays under the path's row until
    /// the next open.
    pub(super) fn reveal_path(&mut self, path: String) {
        self.revealed = reveal(&path).err().map(|note| (path, note));
    }

    /// The message a path click sends.
    pub(super) fn reveal_message(
        &self,
        context: &mut ViewContext<Self>,
        path: &str,
    ) -> Callback<()> {
        context.message(Message::Reveal(path.to_owned()))
    }

    /// Why the last open of `path` failed, in the error color; empty
    /// otherwise.
    pub(super) fn reveal_note(&self, path: &str, palette: Palette) -> View {
        match &self.revealed {
            Some((failed, note)) if failed == path => m3::secondary(note.clone())
                .foreground(palette.error())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .into(),
            _ => View::empty(),
        }
    }

    /// [`path_row`] with its failure note under it.
    pub(super) fn path_row(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
        title: &str,
        note: &str,
        path: &str,
    ) -> View {
        let open = self.reveal_message(context, path);
        StackPanel::new().children((
            path_row(palette, title, note, path, open),
            self.reveal_note(path, palette),
        ))
    }

    /// [`path_link`] with its failure note under it.
    pub(super) fn path_link(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
        path: &str,
    ) -> View {
        let open = self.reveal_message(context, path);
        StackPanel::new().children((
            path_link(palette, path, open),
            self.reveal_note(path, palette),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_is_selected_and_a_folder_is_opened() {
        let file = Target::File(r"C:\work\a b\notes.md".to_owned());
        assert_eq!(explorer_arg(&file), r#"/select,"C:\work\a b\notes.md""#);
        let folder = Target::Folder(r"C:\work\a,b".to_owned());
        assert_eq!(explorer_arg(&folder), r#""C:\work\a,b""#);
        // A drive root's backslash would escape the closing quote.
        let root = Target::Folder(r"C:\".to_owned());
        assert_eq!(explorer_arg(&root), r#""C:\.""#);
    }

    #[test]
    fn explorer_never_sees_the_verbatim_prefix() {
        assert_eq!(explorer_path(r"\\?\C:\work\x.md"), r"C:\work\x.md");
        assert_eq!(
            explorer_path(r"\\?\UNC\host\share\dir"),
            r"\\host\share\dir"
        );
        assert_eq!(explorer_path(r"C:\work"), r"C:\work");
        assert_eq!(explorer_path(r"\\host\share"), r"\\host\share");
    }

    #[test]
    fn existing_files_folders_and_missing_paths_resolve_as_explorer_reads_them() {
        let root =
            std::env::temp_dir().join(format!("swarm-reveal-{}", crate::client::new_mutation_id()));
        let folder = root.join("brief folder");
        std::fs::create_dir_all(&folder).unwrap();
        let file = folder.join("01-first.md");
        std::fs::write(&file, "brief").unwrap();
        let canonical_folder =
            explorer_path(&std::fs::canonicalize(&folder).unwrap().to_string_lossy());

        // Given with the verbatim prefix, as the daemon stores workspaces.
        let verbatim = format!(r"\\?\{}", file.display());
        match target(&verbatim).unwrap() {
            Target::File(path) => {
                assert!(!path.starts_with(r"\\?\"));
                assert!(path.ends_with(r"brief folder\01-first.md"), "{path}");
            }
            other => panic!("a file resolved as {other:?}"),
        }
        assert_eq!(
            target(&folder.to_string_lossy()).unwrap(),
            Target::Folder(canonical_folder)
        );
        let missing = folder.join("gone.md");
        let note = target(&missing.to_string_lossy()).unwrap_err();
        assert!(note.contains("gone.md is not on this computer"), "{note}");
        assert!(!note.contains(r"\\?\"), "{note}");
        assert_eq!(target("  ").unwrap_err(), "No path to open.");

        std::fs::remove_dir_all(&root).unwrap();
    }
}
