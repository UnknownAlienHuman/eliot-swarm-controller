//! Read-only rules and timers of every namespace in scope.

use super::*;

#[derive(Clone, Debug)]
pub(super) enum AutomationMsg {
    RulesLoaded(u64, Vec<(Option<String>, String, Fallible<RulePage>)>),
    AutomationRefreshRequested,
}

/// Rules and timers read for the Automation screen.
pub(super) struct AutomationState {
    pub(super) rules: Vec<RuleNamespace>,
    pub(super) status: String,
    /// Tags rule loads; an older reply is dropped.
    pub(super) epoch: u64,
}

impl DesktopApp {
    pub(super) fn automation_update(
        &mut self,
        msg: AutomationMsg,
        context: &ComponentContext<Self>,
    ) {
        match msg {
            AutomationMsg::RulesLoaded(epoch, namespaces) => {
                if epoch != self.automation.epoch {
                    return;
                }
                self.automation.rules = namespaces
                    .into_iter()
                    .map(|(project, label, page)| RuleNamespace {
                        project,
                        label,
                        more: page.as_ref().is_ok_and(|(_, more)| *more),
                        rules: page.map(|(rules, _)| rules),
                    })
                    .collect();
                let count: usize = self
                    .automation
                    .rules
                    .iter()
                    .map(|namespace| namespace.rules.as_ref().map_or(0, Vec::len))
                    .sum();
                self.automation.status = format!("{count} rule(s) and timer(s) read.");
            }
            AutomationMsg::AutomationRefreshRequested => self.load_rules(context),
        }
    }

    pub(super) fn load_rules(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let projects: Vec<String> = self
            .nav
            .projects
            .iter()
            .map(|project| project.id.clone())
            .collect();
        self.automation.status = "Reading rules…".to_owned();
        self.automation.epoch += 1;
        let epoch = self.automation.epoch;
        let _task = context.spawn_background(move |_| {
            let mut namespaces = Vec::new();
            client::block_on(async {
                namespaces.push((
                    None,
                    "Global rules".to_owned(),
                    client::list_rules(&data_dir, None)
                        .await
                        .map_err(|error| error.to_string()),
                ));
                for project in projects {
                    let page = client::list_rules(&data_dir, Some(project.clone()))
                        .await
                        .map_err(|error| error.to_string());
                    namespaces.push((Some(project.clone()), format!("Project {project}"), page));
                }
            });
            Message::Automation(AutomationMsg::RulesLoaded(epoch, namespaces))
        });
    }
}
