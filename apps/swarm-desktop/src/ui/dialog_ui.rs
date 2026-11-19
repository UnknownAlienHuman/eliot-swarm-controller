//! One M3 dialog surface and footer in a native modal host. All callers use
//! the same palette and controls; WinUI contains Tab focus and restores it.
//! Reactor 0.100.0 has no key events; Escape uses the existing UI-thread
//! hook. Enter stays native and activates the focused control.
use super::*;
use crate::shortcuts::{Popover, PopoverKey};
use crate::theme::winui::Palette;

#[derive(Default)]
pub(super) struct Focus {
    kind: u8,
    pub(super) epoch: u64,
    pub(super) close: ElementRef<Button>,
    pub(super) primary: ElementRef<Button>,
}

pub(super) fn surface(
    context: &mut ViewContext<DesktopApp>,
    epoch: u64,
    palette: Palette,
    title: String,
    body: View,
    footer: View,
    width: f64,
) -> View {
    ContentDialog::new()
        .is_open(true)
        .on_closed(context.callback(move |_| Message::DialogClosed(epoch)))
        .content(
            m3::card_high(palette)
                .max_width(width)
                .automation_name(title.clone())
                .content(
                    StackPanel::new().spacing(tokens::spacing::MD).children((
                        m3::text(title, tokens::text::TITLE).text_wrapping(TextWrapping::Wrap),
                        ScrollViewer::new()
                            .max_height(tokens::layout::DIALOG_MAX_HEIGHT)
                            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                            .content(body),
                        footer,
                    )),
                ),
        )
}

pub(super) fn actions(close: View, primary: View) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .horizontal_alignment(HorizontalAlignment::Right)
        .spacing(tokens::spacing::SM)
        .children((close, primary))
}

impl DesktopApp {
    pub(super) fn dialog_kind(&self) -> u8 {
        if self.handoff.open {
            1
        } else if self.menu.confirm.is_some() {
            2
        } else if self.menu.removal.is_some() {
            3
        } else if self.menu.moving.is_some() {
            4
        } else if self.settings.cleanup_confirm {
            5
        } else if self.bulk.is_some() {
            6
        } else {
            0
        }
    }

    pub(super) fn sync_dialog(&mut self, context: &ComponentContext<Self>) {
        let kind = self.dialog_kind();
        if kind != self.dialog.kind {
            self.dialog.kind = kind;
            self.dialog.epoch += 1;
            if kind == 0 {
                crate::shortcuts::set_popover(Popover::None);
                if self.main == Main::Conversation {
                    let _ = self.chat.composer_ref.request_focus();
                } else {
                    let _ = self.nav.search_ref.request_focus();
                }
            } else {
                self.close_menu();
                self.close_popover();
                crate::shortcuts::set_popover(Popover::Dialog);
                self.post_dialog_focus(self.dialog.epoch, 4, context);
            }
        }
    }

    fn post_dialog_focus(&self, epoch: u64, tries: u8, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let _ = feed::backoff(60, &|| cancel.is_cancelled());
            Message::DialogFocus(epoch, tries)
        });
    }

    pub(super) fn focus_dialog(&self, epoch: u64, tries: u8, context: &ComponentContext<Self>) {
        if self.dialog.kind == 0 || self.dialog.epoch != epoch {
            return;
        }
        let later = context.sender();
        let accepted = self.dialog.primary.request_focus_result(move |result| {
            if !matches!(result, Ok(true)) {
                let _ = later.send(Message::DialogFocusFallback(epoch, tries));
            }
        });
        if !accepted {
            self.focus_dialog_close(epoch, tries, context);
        }
    }

    pub(super) fn focus_dialog_close(
        &self,
        epoch: u64,
        tries: u8,
        context: &ComponentContext<Self>,
    ) {
        if self.dialog.kind == 0 || self.dialog.epoch != epoch {
            return;
        }
        let later = context.sender();
        let accepted = self.dialog.close.request_focus_result(move |result| {
            if !matches!(result, Ok(true)) && tries > 0 {
                let _ = later.send(Message::DialogFocus(epoch, tries - 1));
            }
        });
        if !accepted && tries > 0 {
            self.post_dialog_focus(epoch, tries - 1, context);
        }
    }

    pub(super) fn dialog_key(&mut self, key: PopoverKey, context: &ComponentContext<Self>) {
        if key != PopoverKey::Escape {
            return;
        }
        match self.dialog_kind() {
            1 => self.handoff_update(handoff_ui::HandoffMsg::Closed, context),
            2 => self.menu_update(MenuMsg::Confirmed(false), context),
            3 if self
                .menu
                .removal
                .as_ref()
                .is_some_and(|removal| removal.error.is_some()) =>
            {
                self.menu_update(MenuMsg::RemovalClosed, context)
            }
            4 => self.move_update(MoveMsg::Closed, context),
            5 => self.settings_update(SettingsMsg::CleanupConfirmed(false), context),
            6 => {
                let message = match self.bulk.as_ref().map(|dialog| dialog.phase) {
                    Some(bulk_ui::Phase::Running { .. }) => BulkMsg::Stop,
                    Some(bulk_ui::Phase::Done) => BulkMsg::Closed,
                    _ => BulkMsg::Confirmed(false),
                };
                self.bulk_update(message, context);
            }
            _ => {}
        }
    }
}
