from pprint import pformat
import pytest
import sqlglot

from ..discovery import sql_as_operations

@pytest.mark.parametrize("sql", [
    "CREATE SCHEMA s",
    "CREATE SCHEMA IF NOT EXISTS s",
    "CREATE UNLOGGED TABLE a (b INT);",
    "CREATE TABLE a (b INT);",
    "CREATE TABLE s.a (b INT);",
    "CREATE TABLE IF NOT EXISTS a (b INT);",
    "CREATE TABLE IF NOT EXISTS s.a (b INT);",
    "CREATE TYPE abc AS ENUM ('A', 'B', 'C')",
    "CREATE TYPE s.abc AS ENUM ('A', 'B', 'C')",
    "CREATE INDEX idx_a ON a (id)",
    "CREATE INDEX CONCURRENTLY idx_a ON a (id)",
    "CREATE INDEX idx_a ON s.a (id)",
    "CREATE INDEX IF NOT EXISTS idx_a ON a (id)",
    "CREATE INDEX IF NOT EXISTS idx_a ON s.a (id)",
    "CREATE TABLE c PARTITION OF a FOR VALUES WITH (MODULUS 8, REMAINDER 0)",
    "CREATE TABLE c PARTITION OF b.a FOR VALUES WITH (MODULUS 8, REMAINDER 0)",
])
def test_expr_as_op(snapshot, sql: str):
    operations = sql_as_operations(sql)
    snapshot.assert_match(pformat(operations, width=150), 'schema')


def test_normal_index():
    sql = "CREATE INDEX idx_a ON a (id)"
    schema = sql_as_operations(sql)
    create_index = next(o for o in schema.operations)
    assert not create_index.is_concurrent
    assert schema.can_be_used_in_transaction

def test_concurrent_index():
    sql = "CREATE INDEX CONCURRENTLY idx_a ON a (id)"
    schema = sql_as_operations(sql)
    create_index = next(o for o in schema.operations)
    assert create_index.is_concurrent
    assert not schema.can_be_used_in_transaction
