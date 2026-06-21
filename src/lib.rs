//! stryke-clickhouse — ClickHouse cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn clickhouse__*` is a JSON-string-in /
//! JSON-string-out wrapper. stryke's FFI bridge resolves these symbols at first
//! `use Clickhouse`, passes a JSON-encoded args dict per call, and copies the
//! returned JSON into a stryke string; `stryke_free_cstring` frees it.
//!
//! Transport is ClickHouse's HTTP interface (default port 8123) over `ureq`
//! (sync, rustls) — no tokio, no OpenSSL. SQL is sent in the POST body; SELECTs
//! request `default_format=JSON` so results come back as `{meta, data, rows,
//! statistics}`. A `ureq::Agent` keep-alive pool is cached per
//! `(base_url, auth, database)` for the life of the stryke process.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Value};

// ── connection cache ────────────────────────────────────────────────────────

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ConnKey {
    base: String,
    auth: String,
    database: String,
}

static AGENTS: OnceCell<Mutex<HashMap<ConnKey, Arc<ureq::Agent>>>> = OnceCell::new();

fn agents() -> &'static Mutex<HashMap<ConnKey, Arc<ureq::Agent>>> {
    AGENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve base URL + Basic auth + database from an opts dict. Accepts an
/// explicit `url` (`https://host:8123`) or `host`/`port`/`tls` parts (default
/// `127.0.0.1:8123`, plaintext). User defaults to `default`; database to
/// `default`.
fn conn_from_opts(opts: &Value) -> ConnKey {
    let base = if let Some(u) = opts.get("url").and_then(|v| v.as_str()) {
        u.trim_end_matches('/').to_string()
    } else {
        let host = opts
            .get("host")
            .and_then(|v| v.as_str())
            .unwrap_or("127.0.0.1");
        let port = opts.get("port").and_then(|v| v.as_i64()).unwrap_or(8123);
        let tls = opts.get("tls").and_then(|v| v.as_bool()).unwrap_or(false);
        let scheme = if tls { "https" } else { "http" };
        format!("{}://{}:{}", scheme, host, port)
    };
    let user = opts
        .get("username")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let pass = opts.get("password").and_then(|v| v.as_str()).unwrap_or("");
    let auth = format!("Basic {}", B64.encode(format!("{}:{}", user, pass)));
    let database = opts
        .get("database")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    ConnKey {
        base,
        auth,
        database,
    }
}

fn agent_for(opts: &Value) -> (Arc<ureq::Agent>, ConnKey) {
    let key = conn_from_opts(opts);
    let mut map = agents().lock();
    let agent = map
        .entry(key.clone())
        .or_insert_with(|| {
            Arc::new(
                ureq::AgentBuilder::new()
                    .timeout_connect(Duration::from_secs(10))
                    .timeout(Duration::from_secs(120))
                    .build(),
            )
        })
        .clone();
    (agent, key)
}

// ── HTTP plumbing ───────────────────────────────────────────────────────────

/// POST `sql` to the HTTP interface. `want_json` appends
/// `default_format=JSON` so SELECTs return a parseable result; DDL/DML pass
/// `false` and get an empty body on success. Returns `(status, body)`.
fn http_sql(opts: &Value, sql: &str, want_json: bool) -> Result<(u16, String)> {
    let (agent, key) = agent_for(opts);
    let mut url = format!("{}/?database={}", key.base, urlencode(&key.database));
    if want_json {
        url.push_str("&default_format=JSON");
    }
    if let Some(params) = opts.get("params").and_then(|p| p.as_object()) {
        for (k, v) in params {
            let val = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            url.push_str(&format!("&{}={}", urlencode(k), urlencode(&val)));
        }
    }
    let resp = agent
        .post(&url)
        .set("Authorization", &key.auth)
        .set("Content-Type", "text/plain; charset=utf-8")
        .send_string(sql);
    match resp {
        Ok(r) => {
            let status = r.status();
            let text = r.into_string().map_err(|e| anyhow!("read body: {}", e))?;
            Ok((status, text))
        }
        Err(ureq::Error::Status(code, r)) => Ok((code, r.into_string().unwrap_or_default())),
        Err(ureq::Error::Transport(t)) => Err(anyhow!("clickhouse {}: {}", url, t)),
    }
}

/// Run a SELECT and return the parsed `{meta, data, rows, statistics}` object.
fn query_json(opts: &Value, sql: &str) -> Result<Value> {
    let (status, text) = http_sql(opts, sql, true)?;
    if (200..300).contains(&status) {
        if text.trim().is_empty() {
            Ok(json!({"data": [], "rows": 0}))
        } else {
            serde_json::from_str(&text).map_err(|e| anyhow!("parse result: {}", e))
        }
    } else {
        Err(anyhow!("{}", text.trim()))
    }
}

/// Run a statement that returns no rows (DDL/DML). 2xx → ok.
fn exec_sql(opts: &Value, sql: &str) -> Result<Value> {
    let (status, text) = http_sql(opts, sql, false)?;
    if (200..300).contains(&status) {
        Ok(json!({"ok": true}))
    } else {
        Err(anyhow!("{}", text.trim()))
    }
}

/// `.data` rows of a query result, as an array.
fn data_rows(result: &Value) -> Value {
    result.get("data").cloned().unwrap_or(json!([]))
}

// ── small extractors ────────────────────────────────────────────────────────

fn str_field<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v.get(k)
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow!("missing {}", k))
}

fn opt_str<'a>(v: &'a Value, k: &str) -> Option<&'a str> {
    v.get(k).and_then(|x| x.as_str())
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call<F>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Result<Value>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| handler(input)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-clickhouse handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── version + liveness ──────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn clickhouse__version(args: *const c_char) -> *const c_char {
    ffi_call(args, |_| Ok(json!({"version": env!("CARGO_PKG_VERSION")})))
}

#[no_mangle]
pub extern "C" fn clickhouse__ping(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let (agent, key) = agent_for(&v);
        let url = format!("{}/ping", key.base);
        match agent.get(&url).call() {
            Ok(r) => Ok(json!({"value": r.status() == 200})),
            Err(ureq::Error::Status(_, _)) => Ok(json!({"value": false})),
            Err(e) => Err(anyhow!("ping {}: {}", url, e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__server_version(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let r = query_json(&v, "SELECT version() AS version")?;
        let ver = data_rows(&r)
            .get(0)
            .and_then(|row| row.get("version"))
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({"value": ver}))
    })
}

// ── query ───────────────────────────────────────────────────────────────────

/// Run a SELECT; returns the full `{meta, data, rows, statistics}` object.
#[no_mangle]
pub extern "C" fn clickhouse__query(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = str_field(&v, "sql")?;
        query_json(&v, sql)
    })
}

/// Run a SELECT; returns just the `data` array of row objects.
#[no_mangle]
pub extern "C" fn clickhouse__query_rows(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = str_field(&v, "sql")?;
        let r = query_json(&v, sql)?;
        Ok(json!({ "value": data_rows(&r) }))
    })
}

/// Run a SELECT; returns the first row object (or null).
#[no_mangle]
pub extern "C" fn clickhouse__query_row(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = str_field(&v, "sql")?;
        let r = query_json(&v, sql)?;
        let row = data_rows(&r).get(0).cloned().unwrap_or(Value::Null);
        Ok(json!({ "value": row }))
    })
}

/// Run a SELECT; returns the first column of the first row (a scalar).
#[no_mangle]
pub extern "C" fn clickhouse__query_value(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = str_field(&v, "sql")?;
        let r = query_json(&v, sql)?;
        let val = data_rows(&r)
            .get(0)
            .and_then(|row| row.as_object())
            .and_then(|o| o.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({ "value": val }))
    })
}

/// Run a statement that returns no rows (CREATE/INSERT/ALTER/DROP/…).
#[no_mangle]
pub extern "C" fn clickhouse__exec(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = str_field(&v, "sql")?;
        exec_sql(&v, sql)
    })
}

/// Bulk insert rows into a table. `rows` is an array of objects; the body is
/// `INSERT INTO <table> FORMAT JSONEachRow\n<row>\n…`.
#[no_mangle]
pub extern "C" fn clickhouse__insert(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        let rows = v
            .get("rows")
            .and_then(|r| r.as_array())
            .ok_or_else(|| anyhow!("missing rows array"))?;
        let mut body = format!("INSERT INTO {} FORMAT JSONEachRow\n", table);
        for row in rows {
            body.push_str(&row.to_string());
            body.push('\n');
        }
        exec_sql(&v, &body)?;
        Ok(json!({"ok": true, "count": rows.len()}))
    })
}

// ── schema introspection ────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn clickhouse__databases(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let r = query_json(&v, "SELECT name FROM system.databases ORDER BY name")?;
        let names: Vec<Value> = data_rows(&r)
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|row| row.get("name").cloned())
                    .collect()
            })
            .unwrap_or_default();
        Ok(json!({ "value": names }))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__tables(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let db = opt_str(&v, "database").unwrap_or("default");
        let sql = format!(
            "SELECT name FROM system.tables WHERE database = '{}' ORDER BY name",
            escape_string(db)
        );
        let r = query_json(&v, &sql)?;
        let names: Vec<Value> = data_rows(&r)
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|row| row.get("name").cloned())
                    .collect()
            })
            .unwrap_or_default();
        Ok(json!({ "value": names }))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__describe(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        let r = query_json(&v, &format!("DESCRIBE TABLE {}", table))?;
        Ok(json!({ "value": data_rows(&r) }))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__count(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        let r = query_json(&v, &format!("SELECT count() AS c FROM {}", table))?;
        let n = data_rows(&r)
            .get(0)
            .and_then(|row| row.get("c"))
            .cloned()
            .unwrap_or(json!(0));
        Ok(json!({ "value": n }))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__table_exists(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        let db = opt_str(&v, "database").unwrap_or("default");
        let sql = format!(
            "SELECT count() AS c FROM system.tables WHERE database = '{}' AND name = '{}'",
            escape_string(db),
            escape_string(table)
        );
        let r = query_json(&v, &sql)?;
        let exists = data_rows(&r)
            .get(0)
            .and_then(|row| row.get("c"))
            .map(|c| c.as_i64().unwrap_or(0) > 0 || c.as_str() == Some("1"))
            .unwrap_or(false);
        Ok(json!({ "value": exists }))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__settings(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let r = query_json(
            &v,
            "SELECT name, value, changed, description FROM system.settings ORDER BY name",
        )?;
        Ok(json!({ "value": data_rows(&r) }))
    })
}

// ── DDL helpers ─────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn clickhouse__create_database(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        exec_sql(&v, &format!("CREATE DATABASE IF NOT EXISTS {}", name))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__drop_database(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let name = str_field(&v, "name")?;
        exec_sql(&v, &format!("DROP DATABASE IF EXISTS {}", name))
    })
}

/// Create a table. Pass either a full `sql` CREATE statement, or `name` +
/// `columns` (a `"col Type, …"` string) + optional `engine` (default
/// `MergeTree`) + `order_by`.
#[no_mangle]
pub extern "C" fn clickhouse__create_table(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        if let Some(sql) = opt_str(&v, "sql") {
            return exec_sql(&v, sql);
        }
        let name = str_field(&v, "name")?;
        let columns = str_field(&v, "columns")?;
        let engine = opt_str(&v, "engine").unwrap_or("MergeTree");
        let order_by = opt_str(&v, "order_by").unwrap_or("tuple()");
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = {} ORDER BY {}",
            name, columns, engine, order_by
        );
        exec_sql(&v, &sql)
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__drop_table(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        exec_sql(&v, &format!("DROP TABLE IF EXISTS {}", table))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__truncate_table(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        exec_sql(&v, &format!("TRUNCATE TABLE IF EXISTS {}", table))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__optimize(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let table = str_field(&v, "table")?;
        let mut sql = format!("OPTIMIZE TABLE {}", table);
        if v.get("final").and_then(|x| x.as_bool()).unwrap_or(false) {
            sql.push_str(" FINAL");
        }
        exec_sql(&v, &sql)
    })
}

/// Generic escape hatch: POST arbitrary `sql`; `json` controls whether the
/// result is parsed (SELECT) or treated as a no-row statement.
#[no_mangle]
pub extern "C" fn clickhouse__raw(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let sql = str_field(&v, "sql")?;
        let want_json = v.get("json").and_then(|x| x.as_bool()).unwrap_or(true);
        if want_json {
            query_json(&v, sql)
        } else {
            exec_sql(&v, sql)
        }
    })
}

// ── pure helpers (no network) ───────────────────────────────────────────────

/// Escape a string for a single-quoted ClickHouse SQL literal (`'` → `\'`,
/// `\` → `\\`).
#[no_mangle]
pub extern "C" fn clickhouse__escape_string(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "value": escape_string(s) }))
    })
}

/// Backtick-quote an identifier (`` ` `` → `` \` ``), e.g. a column with a
/// reserved name.
#[no_mangle]
pub extern "C" fn clickhouse__quote_identifier(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "value": quote_identifier(s) }))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__build_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let key = conn_from_opts(&v);
        Ok(json!({ "value": key.base }))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__parse_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = str_field(&v, "url")?;
        Ok(parse_url(url))
    })
}

#[no_mangle]
pub extern "C" fn clickhouse__redact_url(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let url = str_field(&v, "url")?;
        Ok(json!({ "value": redact_url(url) }))
    })
}

/// Wrap a string as a single-quoted ClickHouse string literal.
#[no_mangle]
pub extern "C" fn clickhouse__quote_literal(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "value": quote_literal(s) }))
    })
}

/// Format a JSON value as a ClickHouse literal (string/number/bool/null/array).
#[no_mangle]
pub extern "C" fn clickhouse__format_value(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let val = v.get("value").ok_or_else(|| anyhow!("missing value"))?;
        Ok(json!({ "value": format_value(val) }))
    })
}

/// Render an array of values into an `IN (...)` list.
#[no_mangle]
pub extern "C" fn clickhouse__format_in_list(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let vals = v
            .get("values")
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("missing values array"))?;
        Ok(json!({ "value": format_in_list(vals) }))
    })
}

/// Render an array of values into a ClickHouse array literal `[...]`.
#[no_mangle]
pub extern "C" fn clickhouse__format_array(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let vals = v
            .get("values")
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("missing values array"))?;
        Ok(json!({ "value": format_array(vals) }))
    })
}

/// True when a string is a valid unquoted ClickHouse identifier.
#[no_mangle]
pub extern "C" fn clickhouse__valid_identifier(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "valid": valid_identifier(s) }))
    })
}

/// Escape a string to match literally inside a `LIKE` pattern (`\`, `%`, `_`).
#[no_mangle]
pub extern "C" fn clickhouse__escape_like(args: *const c_char) -> *const c_char {
    ffi_call(args, |v| {
        let s = str_field(&v, "value")?;
        Ok(json!({ "value": escape_like(s) }))
    })
}

// ── shared pure logic (unit-tested) ─────────────────────────────────────────

fn escape_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            _ => out.push(ch),
        }
    }
    out
}

fn quote_identifier(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('`');
    for ch in s.chars() {
        if ch == '`' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('`');
    out
}

/// Wrap a string as a single-quoted ClickHouse string literal (backslash-escaped).
fn quote_literal(s: &str) -> String {
    format!("'{}'", escape_string(s))
}

/// Format a JSON value as a ClickHouse literal: string→`'...'`, number→as-is,
/// bool→`true`/`false`, null→`NULL`, array→`[...]`.
fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_literal(s),
        Value::Array(a) => format_array(a),
        Value::Object(_) => quote_literal(&v.to_string()),
    }
}

/// Render values into an `IN (...)` list. Empty → `(NULL)` (matches nothing).
fn format_in_list(vals: &[Value]) -> String {
    if vals.is_empty() {
        return "(NULL)".to_string();
    }
    format!(
        "({})",
        vals.iter().map(format_value).collect::<Vec<_>>().join(", ")
    )
}

/// Render values into a ClickHouse array literal `[...]`.
fn format_array(vals: &[Value]) -> String {
    format!(
        "[{}]",
        vals.iter().map(format_value).collect::<Vec<_>>().join(", ")
    )
}

/// A ClickHouse identifier is safe unquoted when it matches `[A-Za-z_][A-Za-z0-9_]*`.
fn valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Escape a string to match literally inside a ClickHouse `LIKE` pattern: the
/// wildcards `%`/`_` and the escape char `\` are each backslash-escaped. The
/// result is the pattern body — wrap it with surrounding `%` and quote_literal,
/// e.g. `column LIKE '%' || quote_literal(escape_like(s)) || '%'`.
fn escape_like(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

fn parse_url(url: &str) -> Value {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (s.to_string(), r),
        None => ("http".to_string(), url),
    };
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{}", p)),
        None => (rest, String::new()),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (username, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(u.to_string()), Some(p.to_string())),
            None => (Some(ui.to_string()), None),
        },
        None => (None, None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<i64>().ok()),
        None => (hostport.to_string(), None),
    };
    let tls = scheme == "https";
    // a leading "/<db>" path names the database
    let database = path.trim_start_matches('/');
    json!({
        "scheme": scheme,
        "host": host,
        "port": port.unwrap_or(if tls { 8443 } else { 8123 }),
        "username": username,
        "password": password,
        "database": if database.is_empty() { Value::Null } else { Value::String(database.to_string()) },
        "tls": tls,
    })
}

fn redact_url(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, hostpart)) => format!("{}://***@{}", scheme, hostpart),
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_string_quotes_and_backslash() {
        assert_eq!(escape_string("a'b"), "a\\'b");
        assert_eq!(escape_string("a\\b"), "a\\\\b");
        assert_eq!(escape_string("plain"), "plain");
    }

    #[test]
    fn quote_identifier_backticks() {
        assert_eq!(quote_identifier("col"), "`col`");
        assert_eq!(quote_identifier("we`ird"), "`we\\`ird`");
    }

    #[test]
    fn conn_defaults_to_default_user_and_db() {
        let k = conn_from_opts(&json!({"host": "h"}));
        assert_eq!(k.base, "http://h:8123");
        assert_eq!(k.database, "default");
        let decoded = B64.decode(k.auth.trim_start_matches("Basic ")).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "default:");
    }

    #[test]
    fn conn_url_and_credentials() {
        let k = conn_from_opts(&json!({
            "url": "https://ch:8443/",
            "username": "u",
            "password": "p",
            "database": "metrics"
        }));
        assert_eq!(k.base, "https://ch:8443");
        assert_eq!(k.database, "metrics");
        let decoded = B64.decode(k.auth.trim_start_matches("Basic ")).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "u:p");
    }

    #[test]
    fn parse_url_full() {
        let v = parse_url("https://u:p@ch.example.com:8443/analytics");
        assert_eq!(v["scheme"], "https");
        assert_eq!(v["host"], "ch.example.com");
        assert_eq!(v["port"], 8443);
        assert_eq!(v["username"], "u");
        assert_eq!(v["database"], "analytics");
        assert_eq!(v["tls"], true);
    }

    #[test]
    fn parse_url_bare_host_defaults() {
        let v = parse_url("localhost");
        assert_eq!(v["port"], 8123);
        assert_eq!(v["tls"], false);
        assert_eq!(v["database"], Value::Null);
    }

    #[test]
    fn redact_strips_userinfo() {
        assert_eq!(redact_url("https://u:p@ch:8123"), "https://***@ch:8123");
        assert_eq!(redact_url("http://ch:8123"), "http://ch:8123");
    }

    #[test]
    fn data_rows_extracts_array() {
        let r = json!({"meta": [], "data": [{"x": 1}, {"x": 2}], "rows": 2});
        assert_eq!(data_rows(&r), json!([{"x": 1}, {"x": 2}]));
        assert_eq!(data_rows(&json!({})), json!([]));
    }

    #[test]
    fn quote_literal_backslash_escapes() {
        assert_eq!(quote_literal("a'b"), "'a\\'b'");
        assert_eq!(quote_literal("c\\d"), "'c\\\\d'");
        assert_eq!(quote_literal("plain"), "'plain'");
    }

    #[test]
    fn format_value_by_type() {
        assert_eq!(format_value(&json!(7)), "7");
        assert_eq!(format_value(&json!(true)), "true");
        assert_eq!(format_value(&Value::Null), "NULL");
        assert_eq!(format_value(&json!("a'b")), "'a\\'b'");
        assert_eq!(format_value(&json!([1, "x"])), "[1, 'x']");
    }

    #[test]
    fn format_in_list_and_array() {
        assert_eq!(format_in_list(&[json!(1), json!("a")]), "(1, 'a')");
        assert_eq!(format_in_list(&[]), "(NULL)");
        assert_eq!(format_array(&[json!(1), json!(2)]), "[1, 2]");
    }

    #[test]
    fn valid_identifier_allows_leading_underscore() {
        assert!(valid_identifier("_col1"));
        assert!(valid_identifier("Col"));
        assert!(!valid_identifier("1col"));
        assert!(!valid_identifier("a b"));
    }

    #[test]
    fn escape_like_escapes_wildcards() {
        assert_eq!(escape_like("100%"), "100\\%");
        assert_eq!(escape_like("a_b"), "a\\_b");
        assert_eq!(escape_like("c\\d"), "c\\\\d");
        assert_eq!(escape_like("plain"), "plain");
    }
}
