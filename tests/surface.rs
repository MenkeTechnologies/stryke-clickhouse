//! Integration-test placeholder.
//!
//! `stryke-clickhouse` is a `cdylib`-only crate (no `rlib`), so a `tests/`
//! integration test cannot link against its `extern "C"` exports. The real
//! coverage is:
//!
//!   * `src/lib.rs` `#[cfg(test)] mod tests` — unit tests for the pure logic
//!     (SQL string/identifier escaping, URL parse/redact, connection key,
//!     result-row extraction). These run on `cargo test`.
//!   * `t/test_stryke_clickhouse_surface.stk` — pins that every `Clickhouse::*`
//!     wrapper resolves, with no server required.
//!   * `t/test_clickhouse.stk` — end-to-end DDL + insert + query against a live
//!     ClickHouse at `$CLICKHOUSE_URL`, short-circuited when none answers.

#[test]
fn cdylib_crate_compiles() {
    // Reaching this test means every `extern "C"` `clickhouse__*` export
    // type-checked and linked into the test harness's dependency graph.
}
