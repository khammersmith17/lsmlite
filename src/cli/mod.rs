use crate::config::NldbConfig;
use crate::constants;
use crate::error::NldbError;
use std::collections::HashMap;
use std::env::{self, Args};

/*
*   -- max_memtable-nodes
    -- max_memtable-size
    -- compaction-rate
    -- cache-size
    -- compaction-queue-size
    -- memtable-flush-queue_size
    -- config-file-path
* */

enum ArgValue {
    U64(u64),
    ConfigPath,
}

fn parse_arg(arg_name: &str, cli_args: &mut Args) -> Result<ArgValue, NldbError> {
    match arg_name {
        "--max_memtable-nodes"
        | "--max_memtable-size"
        | "--compaction-rate"
        | "--cache-size"
        | "--cache-shards"
        | "--compaction-queue-size"
        | "--memtable-flush-queue_size" => {
            let Some(arg_value) = cli_args.next() else {
                return Err(NldbError::InvalidParameter);
            };
            Ok(ArgValue::U64(arg_value.parse::<u64>()?))
        }
        "--from-config-file" => Ok(ArgValue::ConfigPath),
        _ => Err(NldbError::InvalidParameter),
    }
}

pub fn parse_config() -> Result<NldbConfig, NldbError> {
    let mut cli_args = env::args();

    let _ = cli_args.next();
    let mut parsed_args: HashMap<String, u64> = HashMap::new();

    while let Some(arg_name) = cli_args.next() {
        let value = parse_arg(&arg_name, &mut cli_args)?;
        match value {
            ArgValue::ConfigPath => {
                return parse_from_config_file();
            }
            ArgValue::U64(v) => parsed_args.insert(arg_name, v),
        };
    }
    Ok(parsed_args.into())
}

fn parse_from_config_file() -> Result<NldbConfig, NldbError> {
    let contents = std::fs::read(constants::NLDB_CONFIG_FILE_PATH)?;
    let config = yaml_serde::from_slice::<NldbConfig>(&contents)?;
    Ok(config)
}
