//! Transactional index of the existing organization grouping rules.

use super::organization_group;

fn refresh(predicate: &str) -> String {
    format!(
        "INSERT INTO organization_membership(agent, project)
         SELECT agent, project FROM organization_membership_source WHERE {predicate}
         ON CONFLICT(agent) DO UPDATE SET project = excluded.project
         WHERE organization_membership.project IS NOT excluded.project;"
    )
}

// Grouping can use an Agent's own evidence, its direct parent, or its root.
// The latter two use their own evidence, so this does not traverse recursively.
fn refresh_related(changed: &str) -> String {
    refresh(&format!(
        "agent IN (
             WITH changed(id) AS MATERIALIZED ({changed})
             SELECT id FROM changed
             UNION SELECT a.id FROM changed c CROSS JOIN agents a ON a.parent = c.id
             UNION SELECT a.id FROM changed c CROSS JOIN agents a ON a.root = c.id
         )"
    ))
}

pub(crate) fn membership_index_migration() -> String {
    let all = refresh("1");
    let agent_new = refresh_related("SELECT NEW.id");
    let agent_old = refresh_related("SELECT OLD.id");
    let workspace_new = refresh_related(
        "SELECT NEW.agent UNION SELECT id FROM agents WHERE workspace = NEW.worktree_path",
    );
    let workspace_old = refresh_related(
        "SELECT OLD.agent UNION SELECT id FROM agents WHERE workspace = OLD.worktree_path",
    );
    let workspace_changed = refresh_related(
        "SELECT OLD.agent UNION SELECT NEW.agent
         UNION SELECT id FROM agents WHERE workspace IN (OLD.worktree_path, NEW.worktree_path)",
    );
    let overlay_new = refresh("agent = NEW.agent");
    let overlay_old = refresh("agent = OLD.agent");
    let overlay_changed = refresh("agent IN (OLD.agent, NEW.agent)");
    format!(
        "CREATE VIEW organization_membership_source AS
             SELECT a.id AS agent, {} AS project
             FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id;
         CREATE TABLE organization_membership (
             agent INTEGER PRIMARY KEY NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
             project TEXT
         ) WITHOUT ROWID;
         CREATE INDEX organization_membership_project ON organization_membership(project, agent);
         CREATE INDEX organization_workspace_borrowers ON agents(workspace) WHERE workspace IS NOT NULL;
         {all}
         CREATE TRIGGER organization_agent_insert AFTER INSERT ON agents
             BEGIN {agent_new} END;
         CREATE TRIGGER organization_agent_update
             AFTER UPDATE OF project, workspace, chat, parent, root ON agents
             WHEN OLD.project IS NOT NEW.project OR OLD.workspace IS NOT NEW.workspace
               OR OLD.chat IS NOT NEW.chat OR OLD.parent IS NOT NEW.parent OR OLD.root IS NOT NEW.root
             BEGIN {agent_new} END;
         CREATE TRIGGER organization_agent_delete AFTER DELETE ON agents
             BEGIN {agent_old} END;
         CREATE TRIGGER organization_workspace_insert AFTER INSERT ON workspaces
             BEGIN {workspace_new} END;
         CREATE TRIGGER organization_workspace_update
             AFTER UPDATE OF agent, worktree_path, repo_common_dir ON workspaces
             WHEN OLD.agent IS NOT NEW.agent OR OLD.worktree_path IS NOT NEW.worktree_path
               OR OLD.repo_common_dir IS NOT NEW.repo_common_dir
             BEGIN {workspace_changed} END;
         CREATE TRIGGER organization_workspace_delete AFTER DELETE ON workspaces
             BEGIN {workspace_old} END;
         CREATE TRIGGER organization_overlay_insert AFTER INSERT ON chat_organization
             BEGIN {overlay_new} END;
         CREATE TRIGGER organization_overlay_update AFTER UPDATE OF agent, project_link ON chat_organization
             WHEN OLD.agent IS NOT NEW.agent OR OLD.project_link IS NOT NEW.project_link
             BEGIN {overlay_changed} END;
         CREATE TRIGGER organization_overlay_delete AFTER DELETE ON chat_organization
             BEGIN {overlay_old} END;
         CREATE TRIGGER organization_registration_insert AFTER INSERT ON project_registrations
             BEGIN {all} END;
         CREATE TRIGGER organization_registration_update
             AFTER UPDATE OF id, checkout, repo_common_dir ON project_registrations
             WHEN OLD.id IS NOT NEW.id OR OLD.checkout IS NOT NEW.checkout
               OR OLD.repo_common_dir IS NOT NEW.repo_common_dir
             BEGIN {all} END;
         CREATE TRIGGER organization_registration_delete AFTER DELETE ON project_registrations
             BEGIN {all} END;",
        organization_group()
    )
}
