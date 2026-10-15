use std::process::Command;

#[test]
fn rejected_cli_arguments_emit_the_owner_error_code_wire_record() {
    let output = Command::new(env!("CARGO_BIN_EXE_swarm-adapter-claude"))
        .arg("--invalid-argument")
        .output()
        .expect("start the actual adapter at its argument boundary");
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let record: serde_json::Value =
        serde_json::from_slice(&output.stderr).expect("stderr is one structured owner record");
    assert_eq!(record["error_code"], "ADAPTER_USAGE");
    assert_eq!(record.as_object().expect("error record object").len(), 1);
}
