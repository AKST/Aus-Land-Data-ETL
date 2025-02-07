from pprint import pformat
import pytest
import sqlglot

from ..discovery import sql_as_operations
from ..type import Ref

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
    "CREATE TABLE a.c PARTITION OF b.a FOR VALUES WITH (MODULUS 8, REMAINDER 0)",
    "DO $$ DECLARE total INT := 1; FOR i in 0..total LOOP\n EXECUTE 'select 1';\nEND LOOP; END $$;",
    "CREATE FUNCTION a.c(input TEXT) RETURNS TEXT $$ BEGIN RETURN ''; END; $$ LANGUAGE plpgsql",
    "CREATE OR REPLACE FUNCTION a.c(input TEXT) RETURNS TEXT $$ BEGIN RETURN ''; END; $$ LANGUAGE plpgsql",
    "ALTER TABLE a ADD CONSTRAINT fk_a_aa_b_bb FOREIGN KEY (aa) REFERENCES b(bb)",
    "ALTER TABLE n.a ADD CONSTRAINT fk_n_a_aa_b_bb FOREIGN KEY (aa) REFERENCES b(bb)",
    "ALTER TABLE a ADD CONSTRAINT fk_a_aa_n_b_bb FOREIGN KEY (aa) REFERENCES n.b(bb)",
    "ALTER TABLE n.a ADD CONSTRAINT fk_n_a_aa_n_b_bb FOREIGN KEY (aa) REFERENCES n.b(bb)",
])
def test_expr_as_op(snapshot, sql: str):
    operations = sql_as_operations(sql)
    snapshot.assert_match(pformat(operations, width=150), 'schema')

@pytest.mark.parametrize("sql,fk_name,src_ref,ref_ref,src_col,ref_col", [
    (
        "ALTER TABLE a ADD CONSTRAINT fk_a_aa_b_bb FOREIGN KEY (aa) REFERENCES b(bb)",
        'fk_a_aa_b_bb', Ref(None, 'a'), Ref(None, 'b'), 'aa', 'bb'
    ),
    (
        "ALTER TABLE n.a ADD CONSTRAINT fk_n_a_aa_b_bb FOREIGN KEY (aa) REFERENCES b(bb)",
        'fk_n_a_aa_b_bb', Ref('n', 'a'), Ref(None, 'b'), 'aa', 'bb'
    ),
    (
        "ALTER TABLE a ADD CONSTRAINT fk_a_aa_n_b_bb FOREIGN KEY (aa) REFERENCES m.b(bb)",
        'fk_a_aa_n_b_bb', Ref(None, 'a'), Ref('m', 'b'), 'aa', 'bb'
    ),
    (
        "ALTER TABLE n.a ADD CONSTRAINT fk_n_a_aa_n_b_bb FOREIGN KEY (aa) REFERENCES m.b(bb)",
        'fk_n_a_aa_n_b_bb', Ref('n', 'a'), Ref('m', 'b'), 'aa', 'bb'
    ),
])
def test_alter_table(sql, fk_name, src_ref, ref_ref, src_col, ref_col):
    s = next(o for o in sql_as_operations(sql).operations)
    assert s.altered_table == src_ref
    assert s.actions[0].constraint_name == fk_name
    assert s.actions[0].constraint_column == src_col
    assert s.actions[0].ref_table == ref_ref
    assert s.actions[0].ref_column == ref_col

@pytest.mark.parametrize("sql", [
    "CREATE FUNCTION a.c(input TEXT) RETURNS TEXT $$ BEGIN RETURN ''; END; $$ LANGUAGE plpgsql",
    "CREATE OR REPLACE FUNCTION a.c(input TEXT) RETURNS TEXT $$ BEGIN RETURN ''; END; $$ LANGUAGE plpgsql",
])
def test_create_function_name_and_namespace(sql):
    schema = sql_as_operations(sql)
    create_function = next(o for o in schema.operations)
    assert create_function.func.schema_name == 'a'
    assert create_function.func.name == 'c'

def test_schema_for_parition():
    sql = \
        "CREATE TABLE a.c PARTITION OF b.a FOR VALUES WITH (MODULUS 8, REMAINDER 0)"
    schema = sql_as_operations(sql)
    create_parition = next(o for o in schema.operations)
    assert create_parition.partition.schema_name == 'a'

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
