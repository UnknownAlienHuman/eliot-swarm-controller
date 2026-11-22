use super::*;

pub struct IntegrationCustody {
    pub command_id: String,
    pub scratch_id: String,
    pub data_root: String,
    pub checkout: String,
    pub common_dir: String,
    pub boot_id: String,
    pub armed: bool,
    pub process: Option<core::ProcessIdentity>,
}

impl Store {
    /// Reserve an exact scratch name before creating any filesystem state.
    pub fn reserve_integration_custody(&mut self, record: &IntegrationCustody) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self.connection.execute(
            "INSERT INTO git_integration_custody
                 (command_id,scratch_id,data_root,checkout,common_dir,boot_id)
             SELECT id,?2,?3,?4,?5,?6 FROM commands
             WHERE id=?1 AND verb='git.integrate' AND state='dispatched'
               AND json_extract(args,'$.common_dir')=?5
               AND json_extract(args,'$.canonical_checkout')=?4",
            params![
                record.command_id,
                record.scratch_id,
                record.data_root,
                record.checkout,
                record.common_dir,
                record.boot_id
            ],
        );
        self.check_integration_custody_write(result)
    }

    pub fn integration_custody(&self, command_id: &str) -> Result<Option<IntegrationCustody>> {
        self.integration_custody_rows("WHERE j.command_id = ?1 AND j.cleaned = 0", [command_id])
            .map(|mut rows| rows.pop())
    }

    pub fn recoverable_integration_custody(
        &self,
        common_dir: Option<&str>,
    ) -> Result<Vec<IntegrationCustody>> {
        self.integration_custody_rows(
            "WHERE j.cleaned=0 AND c.state NOT IN ('dispatched','accepted','queued')
               AND (?1 IS NULL OR j.common_dir=?1)",
            [common_dir],
        )
    }

    fn integration_custody_rows(
        &self,
        predicate: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<IntegrationCustody>> {
        self.ensure_committed_reads()?;
        let sql = format!(
            "SELECT j.command_id,j.scratch_id,j.data_root,j.checkout,j.common_dir,j.boot_id,
                    j.armed,j.process_identity
             FROM git_integration_custody j JOIN commands c ON c.id=j.command_id {predicate}"
        );
        let mut stmt = self.connection.prepare(&sql).map_err(sql_error)?;
        let mut rows = stmt.query(params).map_err(sql_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().map_err(sql_error)? {
            let process: Option<String> = row.get(7).map_err(sql_error)?;
            records.push(IntegrationCustody {
                command_id: row.get(0).map_err(sql_error)?,
                scratch_id: row.get(1).map_err(sql_error)?,
                data_root: row.get(2).map_err(sql_error)?,
                checkout: row.get(3).map_err(sql_error)?,
                common_dir: row.get(4).map_err(sql_error)?,
                boot_id: row.get(5).map_err(sql_error)?,
                armed: row.get(6).map_err(sql_error)?,
                process: process
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|_| {
                        Error::new(
                            "source_integrity",
                            "integration process identity is invalid",
                        )
                    })?,
            });
        }
        Ok(records)
    }

    pub fn arm_integration_process(&mut self, id: &str) -> Result<()> {
        self.integration_write(
            "UPDATE git_integration_custody SET armed=1,process_identity=NULL
             WHERE command_id=?1 AND armed=0 AND cleaned=0",
            [id],
        )
    }

    pub fn record_integration_process(
        &mut self,
        id: &str,
        identity: &core::ProcessIdentity,
    ) -> Result<()> {
        self.integration_write(
            "UPDATE git_integration_custody SET process_identity=?2
             WHERE command_id=?1 AND armed=1 AND cleaned=0 AND process_identity IS NULL",
            params![id, encode_json(identity)?],
        )
    }

    pub fn clear_integration_process(&mut self, id: &str) -> Result<()> {
        self.integration_write(
            "UPDATE git_integration_custody SET armed=0,process_identity=NULL
             WHERE command_id=?1 AND cleaned=0",
            [id],
        )
    }

    pub fn mark_integration_ref_pending(&mut self, id: &str, pending: bool) -> Result<()> {
        self.integration_write(
            "UPDATE git_integration_custody SET ref_pending=?2 WHERE command_id=?1 AND cleaned=0",
            params![id, pending],
        )
    }

    pub fn complete_integration_cleanup(&mut self, id: &str) -> Result<()> {
        self.integration_write(
            "UPDATE git_integration_custody SET cleaned=1
             WHERE command_id=?1 AND armed=0 AND cleaned=0",
            [id],
        )
    }

    fn integration_write(&mut self, sql: &str, params: impl rusqlite::Params) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self.connection.execute(sql, params);
        self.check_integration_custody_write(result)
    }

    fn check_integration_custody_write(&mut self, result: rusqlite::Result<usize>) -> Result<()> {
        let result = result.map_err(sql_error).and_then(|count| {
            if count == 1 {
                Ok(())
            } else {
                Err(Error::new("bad_state", "integration custody changed"))
            }
        });
        self.fault_on_persistence_error(&result);
        result
    }
}
