//! Transactional reads of the persisted Git task index.
use super::*;
use eliot_core::api::{TaskListCursor, TaskListItem, TaskListRequest};

pub struct TaskRecordPage {
    pub revision: i64,
    pub tasks: Vec<TaskListItem>,
    pub next_cursor: Option<TaskListCursor>,
}

impl Store {
    pub fn task_record_list(&self, request: &TaskListRequest) -> Result<TaskRecordPage> {
        request.validate()?;
        self.ensure_committed_reads()?;
        let tx = self.connection.unchecked_transaction().map_err(sql_error)?;
        let agent = parse_agent_id(&request.agent)?;
        let revision: i64 = tx
            .query_row(
                "SELECT COALESCE((SELECT revision FROM git_work_task_revisions
             WHERE agent=?1 AND generation=?2),0)",
                params![agent, request.generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let cursor = request.cursor.as_ref();
        if let Some(cursor) = cursor {
            if cursor.revision != revision {
                return Err(Error::new(
                    "stale_cursor",
                    "task list changed; restart the list",
                ));
            }
            let exists = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM git_work_tasks WHERE agent=?1 AND generation=?2 AND task_command=?3)",
                params![agent, request.generation, cursor.after],
                |row| row.get::<_, bool>(0),
            ).map_err(sql_error)?;
            if !exists {
                return Err(Error::new(
                    "invalid_cursor",
                    "task cursor command does not exist",
                ));
            }
        }
        let mut statement = tx
            .prepare(
                "SELECT task_command,task_ref,principal FROM git_work_tasks
             WHERE agent=?1 AND generation=?2 AND task_command>?3
             ORDER BY task_command LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    agent,
                    request.generation,
                    cursor.map_or("", |c| c.after.as_str()),
                    (request.limit + 1) as i64
                ],
                |row| {
                    Ok(TaskListItem {
                        task_command: row.get(0)?,
                        task_ref: row.get(1)?,
                        principal: row.get(2)?,
                    })
                },
            )
            .map_err(sql_error)?;
        let mut tasks = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let more = tasks.len() > request.limit;
        if more {
            tasks.truncate(request.limit);
        }
        let next_cursor = if more {
            let cursor = TaskListCursor {
                agent: request.agent.clone(),
                generation: request.generation,
                revision,
                after: tasks.last().expect("non-empty page").task_command.clone(),
            };
            Some(cursor)
        } else {
            None
        };
        drop(statement);
        tx.commit().map_err(sql_error)?;
        Ok(TaskRecordPage {
            revision,
            tasks,
            next_cursor,
        })
    }

    pub fn task_record_binding(
        &self,
        agent: &str,
        generation: i64,
        command: &str,
    ) -> Result<Option<(String, i64)>> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT task_ref, COALESCE((SELECT revision FROM git_work_task_revisions r
             WHERE r.agent=t.agent AND r.generation=t.generation),0)
             FROM git_work_tasks t WHERE agent=?1 AND generation=?2 AND task_command=?3",
                params![parse_agent_id(agent)?, generation, command],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)
    }
}
