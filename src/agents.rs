use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use modular_agent_kit::{
    MAK, Agent, AgentContext, AgentData, AgentError, AgentOutput, AgentSpec, AgentValue, AsAgent,
    mak_agent, async_trait,
};
use cozo::{DataValue, DbInstance, JsonData, NamedRows, Num, UuidWrapper, Vector};
use im::hashmap;

static DB_MAP: OnceLock<Mutex<BTreeMap<String, DbInstance>>> = OnceLock::new();

static CATEGORY: &str = "DB/CozoDB";

static PORT_ARRAY: &str = "array";
static PORT_KV: &str = "kv";
static PORT_VALUE: &str = "value";
static PORT_TABLE: &str = "table";

static CONFIG_DB: &str = "db";
static CONFIG_SCRIPT: &str = "script";

#[mak_agent(
    title = "CozoDB Script",
    category = CATEGORY,
    inputs = [PORT_KV, PORT_VALUE],
    outputs = [PORT_TABLE],
    string_config(name = CONFIG_DB),
    text_config(name = CONFIG_SCRIPT)
)]
struct CozoDbScriptAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for CozoDbScriptAgent {
    fn new(mak: MAK, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(mak, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let config = self.configs()?;
        let db = get_db_instance(&config.get_string_or_default(CONFIG_DB))?;
        let script = config.get_string(CONFIG_SCRIPT)?;
        if script.is_empty() {
            return Ok(());
        }

        let mut params: BTreeMap<String, cozo::DataValue> = BTreeMap::new();
        if port == PORT_KV {
            if let Some(kv) = value.as_object() {
                for (k, v) in kv {
                    params.insert(k.clone(), v.to_json().into());
                }
            } else {
                return Err(AgentError::InvalidValue(
                    "Expected object for KV input".to_string(),
                ));
            };
        } else if port == PORT_VALUE {
            params.insert("value".to_string(), value.to_json().into());
        }

        let result = db
            .run_script(&script, params, cozo::ScriptMutability::Mutable)
            .map_err(|e| AgentError::IoError(format!("Cozo Error: {}", e)))?;

        let value = named_rows_to_agent_value(result);

        self.output(ctx, PORT_TABLE, value).await
    }
}

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

#[mak_agent(
    title = "Rows",
    category = CATEGORY,
    inputs = [PORT_TABLE],
    outputs = [PORT_ARRAY],
)]
struct RowsAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for RowsAgent {
    fn new(mak: MAK, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(mak, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let rows = value
            .get_array("rows")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'rows' field".to_string()))?;
        self.output(ctx, PORT_ARRAY, AgentValue::array(rows.clone()))
            .await
    }
}

#[mak_agent(
    title = "Row",
    category = CATEGORY,
    inputs = [PORT_TABLE],
    outputs = [PORT_ARRAY],
    integer_config(name = "index"),
)]
struct RowAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for RowAgent {
    fn new(mak: MAK, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(mak, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let index = self.configs()?.get_integer("index")? as usize;
        let row = value
            .get_array("rows")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'rows' field".to_string()))?
            .get(index)
            .ok_or_else(|| {
                AgentError::InvalidValue(format!("Row index {} out of bounds", index))
            })?;
        self.output(ctx, PORT_ARRAY, row.clone()).await
    }
}

#[mak_agent(
    title = "Select",
    category = CATEGORY,
    inputs = [PORT_TABLE],
    outputs = [PORT_ARRAY],
    string_config(name = "cols"),
)]
struct SelectAgent {
    data: AgentData,
}

#[async_trait]
impl AsAgent for SelectAgent {
    fn new(mak: MAK, id: String, spec: AgentSpec) -> Result<Self, AgentError> {
        Ok(Self {
            data: AgentData::new(mak, id, spec),
        })
    }

    async fn process(
        &mut self,
        ctx: AgentContext,
        _port: String,
        value: AgentValue,
    ) -> Result<(), AgentError> {
        let cols = self
            .configs()?
            .get_string("cols")?
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        let headers = value
            .get_array("headers")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'headers' field".to_string()))?;
        let col_indices: Vec<usize> = cols
            .iter()
            .map(|col| {
                headers
                    .iter()
                    .position(|h| h.as_str().map_or(false, |hs| hs == col))
                    .ok_or_else(|| AgentError::InvalidValue(format!("Column '{}' not found", col)))
            })
            .collect::<Result<Vec<usize>, AgentError>>()?;

        let arr = value
            .get_array("rows")
            .ok_or_else(|| AgentError::InvalidValue("Missing 'rows' field".to_string()))?
            .iter()
            .map(|row| {
                let row_array = row
                    .as_array()
                    .ok_or_else(|| AgentError::InvalidValue("Row is not an array".to_string()))?;
                let selected_cells: im::Vector<AgentValue> = col_indices
                    .iter()
                    .map(|&i| {
                        row_array
                            .get(i)
                            .cloned()
                            .unwrap_or_else(|| AgentValue::unit())
                    })
                    .collect();
                Ok(AgentValue::array(selected_cells))
            })
            .collect::<Result<im::Vector<AgentValue>, AgentError>>()?;

        if arr.len() == 1 {
            self.output(ctx, PORT_ARRAY, arr[0].clone()).await
        } else {
            self.output(ctx, PORT_ARRAY, AgentValue::array(arr)).await
        }
    }
}

fn data_value_to_agent_value(value: DataValue) -> AgentValue {
    match value {
        DataValue::Null => AgentValue::unit(),
        DataValue::Bool(b) => AgentValue::boolean(b),
        DataValue::Num(Num::Int(i)) => AgentValue::integer(i),
        DataValue::Num(Num::Float(f)) => AgentValue::number(f),
        DataValue::Str(s) => AgentValue::string(s.to_string()),
        DataValue::Bytes(bytes) => {
            let arr = bytes
                .into_iter()
                .map(|b| AgentValue::integer(b as i64))
                .collect::<im::Vector<_>>();
            AgentValue::array(arr)
        }
        DataValue::Uuid(UuidWrapper(uuid)) => AgentValue::string(uuid.to_string()),
        DataValue::Regex(rx) => AgentValue::string(rx.0.as_str().to_string()),
        DataValue::List(list) => {
            let arr = list
                .into_iter()
                .map(data_value_to_agent_value)
                .collect::<im::Vector<_>>();
            AgentValue::array(arr)
        }
        DataValue::Set(set) => {
            let arr = set
                .into_iter()
                .map(data_value_to_agent_value)
                .collect::<im::Vector<_>>();
            AgentValue::array(arr)
        }
        DataValue::Vec(vec) => {
            let v = match vec {
                Vector::F32(arr) => arr.to_vec(),
                Vector::F64(arr) => arr.iter().map(|&v| v as f32).collect(),
            };
            AgentValue::tensor(v)
        }
        DataValue::Json(JsonData(json)) => {
            let json_string = json.to_string();
            let inner =
                AgentValue::from_json(json).unwrap_or_else(|_| AgentValue::string(json_string));
            inner
        }
        DataValue::Validity(v) => AgentValue::object(hashmap! {
                "timestamp".into() =>
                AgentValue::integer(v.timestamp.0.0),
            "is_assert".into() => AgentValue::boolean(v.is_assert.0),
        }),
        DataValue::Bot => AgentValue::unit(),
    }
}

fn named_rows_to_agent_value(named_rows: NamedRows) -> AgentValue {
    let NamedRows {
        headers,
        rows,
        next,
    } = named_rows;
    let headers_value = AgentValue::array(headers.into_iter().map(AgentValue::string).collect());

    let row_values: im::Vector<AgentValue> = rows
        .into_iter()
        .map(|row| {
            let cells: im::Vector<AgentValue> =
                row.into_iter().map(data_value_to_agent_value).collect();
            AgentValue::array(cells)
        })
        .collect();

    let rows_value = AgentValue::array(row_values);
    let next_value = next
        .map(|n| named_rows_to_agent_value(*n))
        .unwrap_or_else(AgentValue::unit);

    AgentValue::object(hashmap! {
        "headers".into() => headers_value,
        "rows".into() => rows_value,
        "next".into() => next_value,
    })
}

#[cfg(test)]
mod tests {
    use im::vector;

    use super::*;
    use std::cmp::Reverse;
    use std::collections::BTreeSet;

    #[test]
    fn test_data_value_to_agent_value_primitives() {
        assert_eq!(
            data_value_to_agent_value(DataValue::Null),
            AgentValue::unit()
        );
        assert_eq!(
            data_value_to_agent_value(DataValue::Bool(true)),
            AgentValue::boolean(true)
        );
        assert_eq!(
            data_value_to_agent_value(DataValue::Num(Num::Int(42))),
            AgentValue::integer(42)
        );
        assert_eq!(
            data_value_to_agent_value(DataValue::Num(Num::Float(3.5))),
            AgentValue::number(3.5)
        );
        assert_eq!(
            data_value_to_agent_value(DataValue::from("hello")),
            AgentValue::string("hello")
        );
        assert_eq!(
            data_value_to_agent_value(DataValue::Bytes(vec![0, 255])),
            AgentValue::array(vector![AgentValue::integer(0), AgentValue::integer(255)])
        );

        let validity = cozo::Validity {
            timestamp: cozo::ValidityTs(Reverse(123)),
            is_assert: Reverse(true),
        };
        assert_eq!(
            data_value_to_agent_value(DataValue::Validity(validity)),
            AgentValue::object(hashmap! {
                "timestamp".into() => AgentValue::integer(123),
                "is_assert".into() => AgentValue::boolean(true),
            })
        );
        assert_eq!(
            data_value_to_agent_value(DataValue::Bot),
            AgentValue::unit()
        );
    }

    #[test]
    fn test_data_value_to_agent_value_nested() {
        let list = DataValue::List(vec![
            DataValue::Num(Num::Int(1)),
            DataValue::from("x"),
            DataValue::Null,
        ]);
        assert_eq!(
            data_value_to_agent_value(list),
            AgentValue::array(vector![
                AgentValue::integer(1),
                AgentValue::string("x"),
                AgentValue::unit()
            ])
        );

        let set = DataValue::Set(BTreeSet::from([
            DataValue::Num(Num::Int(2)),
            DataValue::Num(Num::Int(1)),
        ]));
        assert_eq!(
            data_value_to_agent_value(set),
            AgentValue::array(vector![AgentValue::integer(1), AgentValue::integer(2)])
        );

        let json_value = serde_json::json!({"a": 1, "b": [true, null]});
        let json_dv = DataValue::from(json_value);
        assert_eq!(
            data_value_to_agent_value(json_dv),
            AgentValue::object(hashmap! {
                "a".into() => AgentValue::integer(1),
                    "b".into() => AgentValue::array(vector![
                        AgentValue::boolean(true),
                        AgentValue::unit()
                    ]),
            })
        );
    }

    #[test]
    fn test_named_rows_to_agent_value() {
        let next = NamedRows::new(
            vec!["n".to_string()],
            vec![vec![DataValue::Num(Num::Int(2))]],
        );

        let rows = NamedRows {
            headers: vec!["a".to_string(), "b".to_string()],
            rows: vec![vec![DataValue::Num(Num::Int(1)), DataValue::from("x")]],
            next: Some(Box::new(next)),
        };

        assert_eq!(
            named_rows_to_agent_value(rows),
            AgentValue::object(hashmap! {
                "headers".into() => AgentValue::array(vector![AgentValue::string("a"), AgentValue::string("b")]),
                "rows".into() => AgentValue::array(vector![
                    AgentValue::array(vector![
                        AgentValue::integer(1),
                        AgentValue::string("x")
                    ])
                ]),
                "next".into() => AgentValue::object(hashmap! {
                    "headers".into() => AgentValue::array(vector![AgentValue::string("n")]),
                    "rows".into() => AgentValue::array(vector![
                        AgentValue::array(vector![AgentValue::integer(2)])
                    ]),
                    "next".into() => AgentValue::unit(),
            }),
            })
        );
    }
}
