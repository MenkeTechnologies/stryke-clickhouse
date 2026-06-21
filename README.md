```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                [ c l i c k h o u s e ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-clickhouse/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-clickhouse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[CLICKHOUSE CLIENT FOR STRYKE // QUERY + INSERT + DDL + INTROSPECTION]`

> *"Columnar OLAP, one stryke pipe at a time."*

ClickHouse client for stryke. Run SELECTs, bulk-insert via JSONEachRow, manage
databases/tables, and introspect the schema against any ClickHouse server over
its HTTP interface (default port 8123). Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-duckdb`](https://github.com/MenkeTechnologies/stryke-duckdb) · [`stryke-postgres`](https://github.com/MenkeTechnologies/stryke-postgres) · [`stryke-search`](https://github.com/MenkeTechnologies/stryke-search)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] Connecting](#0x02-connecting)
- [\[0x03\] Architecture](#0x03-architecture)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Build & test](#0x05-build--test)
- [\[0x06\] License](#0x06-license)

---

## \[0x00\] Install

```sh
s add github.com/MenkeTechnologies/stryke-clickhouse
```

On first `use Clickhouse`, stryke dlopens the cdylib in-process and registers
every `clickhouse__*` export.

---

## \[0x01\] Quick start

```perl
use Clickhouse

var %conn
$conn{url} = "http://127.0.0.1:8123"

Clickhouse::create_table(
    "events",
    columns  => "ts DateTime, user UInt64, kind String",
    order_by => "(ts, user)",
    %conn,
)

Clickhouse::insert("events", [
    { ts => "2026-06-20 12:00:00", user => 1, kind => "click" },
    { ts => "2026-06-20 12:00:01", user => 2, kind => "view"  },
], %conn)

p Clickhouse::count("events", %conn)                              # 2
p Clickhouse::query_value("SELECT uniqExact(user) FROM events", %conn)
val @rows = Clickhouse::query_rows("SELECT kind, count() c FROM events GROUP BY kind", %conn)
```

---

## \[0x02\] Connecting

Connection params come from the `%conn` opts hash on every call (or
`$CLICKHOUSE_URL` when neither `url` nor `host` is given):

| Key        | Default       | Notes                                           |
| ---------- | ------------- | ----------------------------------------------- |
| `url`      | —             | Full base URL, e.g. `https://ch.example.com:8443` |
| `host`     | `127.0.0.1`   | Used with `port`/`tls` when `url` is absent     |
| `port`     | `8123`        | HTTP interface port                             |
| `tls`      | `false`       | `true` selects the `https` scheme               |
| `username` | `default`     | HTTP Basic auth user                            |
| `password` | (empty)       | HTTP Basic auth password                        |
| `database` | `default`     | Sent as `?database=` on every request           |
| `params`   | —             | Extra ClickHouse settings as URL query params   |

A `ureq::Agent` (HTTP keep-alive pool) is cached per `(base_url, auth, database)`
for the life of the stryke process.

---

## \[0x03\] Architecture

- **Transport** — ClickHouse's HTTP interface over [`ureq`](https://docs.rs/ureq):
  synchronous, pure-Rust, rustls-backed. No tokio, no OpenSSL.
- **Formats** — SELECTs request `default_format=JSON` and come back as
  `{ meta, data, rows, statistics }`; inserts send `FORMAT JSONEachRow`.
- **JSON-in / JSON-out FFI** — each `clickhouse__*` export takes a JSON args dict
  and returns JSON; handlers run inside `catch_unwind`.
- **Pure helpers** — `escape`, `quote_ident`, and the URL helpers take no
  connection and are unit-tested in-crate, so they validate in CI without a server.

---

## \[0x04\] API reference

| Group         | Functions                                                                             |
| ------------- | ------------------------------------------------------------------------------------- |
| Liveness      | `version`, `ping`, `server_version`                                                    |
| Query         | `query`, `query_rows`, `query_row`, `query_value`, `exec`, `insert`, `raw`             |
| Introspection | `databases`, `tables`, `describe`, `count`, `table_exists`, `settings`                 |
| DDL           | `create_database`, `drop_database`, `create_table`, `drop_table`, `truncate_table`, `optimize` |
| Pure helpers  | `escape`, `quote_literal`, `quote_ident`, `valid_identifier`, `format_value`, `format_in_list`, `format_array`, `build_url`, `parse_url`, `redact_url` |

`query` returns the full result object; `query_rows` / `query_row` /
`query_value` peel off the array / first row / first scalar. `exec` is for
statements that return no rows.

```perl
# parameterize safely with the escape helper
val $name = Clickhouse::escape($user_input)
val @hits = Clickhouse::query_rows("SELECT * FROM events WHERE kind = '$name'", %conn)
```

---

## \[0x05\] Build & test

```sh
make debug       # cargo build
make test        # cargo test, then `s test t/` (needs $CLICKHOUSE_URL or 127.0.0.1:8123)
make install     # s pkg install -g .
```

`cargo test` runs the in-crate unit tests (string/identifier escaping, URL
parse/redact, connection key, result extraction) with no server required. The
`t/test_stryke_clickhouse_surface.stk` pins the wrapper surface; `t/test_clickhouse.stk`
runs end-to-end DDL + insert + query against a live server and short-circuits
when none answers.

---

## \[0x06\] License

MIT &middot; MenkeTechnologies
