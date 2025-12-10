use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use agent_stream_kit::{
    ASKit, Agent, AgentContext, AgentData, AgentError, AgentOutput, AgentSpec, AgentValue, AsAgent,
    askit_agent, async_trait,
};
use cozo::DbInstance;

static DB_MAP: OnceLock<Mutex<BTreeMap<String, DbInstance>>> = OnceLock::new();

fn get_db_instance(path: &str) -> Result<DbInstance, AgentError> {
    let db_map = DB_MAP.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut map_guard = db_map.lock().unwrap();

    if let Some(db) = map_guard.get(path) {
        return Ok(db.clone());
    }

    let db = if path.is_empty() {
        DbInstance::new("mem", "", "")
    } else {
        DbInstance::new("sqlite", path, "")
    }
    .map_err(|e| AgentError::IoError(format!("Cozo Error: {}", e)))?;

    map_guard.insert(path.to_string(), db.clone());

    Ok(db)
}

static CATEGORY: &str = "CozoDB";

static PORT_PARAMS: &str = "params";
static PORT_RESULT: &str = "result";

static CONFIG_DB: &str = "db";
static CONFIG_SCRIPT: &str = "script";

// CozoDB Script
#[askit_agent(
    title = "CozoDB Script",
    category = CATEGORY,
    inputs = [PORT_PARAMS],
    outputs = [PORT_RESULT],
    string_config(name = CONFIG_DB, title = "Database"),
    text_config(name = CONFIG_SCRIPT, title = "Script")
)]
struct CozoDbScriptAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for CozoDbScriptAgent {
    fn new(askit: ASKit, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(askit, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _pin: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let db = get_db_instance(&config.get_string_or_default(CONFIG_DB))?;
        let script = config.get_string(CONFIG_SCRIPT)?;
        if script.is_empty() {
            return Ok(());
        }

        let params: BTreeMap<String, cozo::DataValue> = if let Some(params) = value.as_object() {
            params
                .iter()
                .map(|(k, v)| (k.clone(), v.to_json().into()))
                .collect()
        } else {
            BTreeMap::new()
        };

        let result = db
            .run_script(&script, params, cozo::ScriptMutability::Mutable)
            .map_err(|e| AgentError::IoError(format!("Cozo Error: {}", e)))?;

        let value = AgentValue::from_serialize(&result)?;

        self.try_output(ctx, PORT_RESULT, value)
    }
}
