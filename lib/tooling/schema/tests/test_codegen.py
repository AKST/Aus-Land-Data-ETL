from pprint import pformat
from typing import List
import pytest

from ..type import Stmt
from ..codegen import *
from ..discovery import sql_as_operations

@pytest.mark.parametrize("omit_foreign_keys,sql", [
    (False, "CREATE SCHEMA s"),
    (False, "CREATE UNLOGGED TABLE a (b_id INT)"),
    (False, "CREATE UNLOGGED TABLE s.a (b_id INT)"),
    (True, "CREATE UNLOGGED TABLE a (b_id INT)"),
    (True, "CREATE UNLOGGED TABLE s.a (b_id INT)"),
    (False, "CREATE TABLE a (b_id INT, FOREIGN KEY (b_id) REFERENCES b(b_id))"),
    (False, "CREATE TABLE s.a (b_id INT, FOREIGN KEY (b_id) REFERENCES b(b_id))"),
    (True, "CREATE TABLE a (b_id INT, FOREIGN KEY (b_id) REFERENCES b(b_id))"),
    (True, "CREATE TABLE s.a (b_id INT, FOREIGN KEY (b_id) REFERENCES b(b_id))"),
    (False, "CREATE TABLE b (c DOUBLE PRECISION)"),
    (False, "CREATE TABLE s.b (c DOUBLE PRECISION)"),
    (False, "CREATE TYPE abc AS ENUM ('A', 'B', 'C')"),
    (False, "CREATE TYPE s.abc AS ENUM ('A', 'B', 'C')"),
    (False, "CREATE INDEX IF NOT EXISTS idx_a ON a (id)"),
    (False, "CREATE INDEX CONCURRENTLY idx_a ON a (id)"),
    (False, "CREATE INDEX IF NOT EXISTS idx_a ON s.a (id)"),
    (False, "CREATE TABLE c PARTITION OF a FOR VALUES WITH (MODULUS 8, REMAINDER 0)"),
    (False, "CREATE TABLE c PARTITION OF b.a FOR VALUES WITH (MODULUS 8, REMAINDER 0)"),
    (False, "DO $$ DECLARE total INT := 1; FOR i in 0..total LOOP\n EXECUTE 'select 1';\nEND LOOP; END $$;"),
])
def test_create(snapshot, omit_foreign_keys: bool, sql: str):
    codegen = list(create(sql_as_operations(sql), omit_foreign_keys))
    snapshot.assert_match(pformat(codegen), sql)

@pytest.mark.parametrize("cascade,sql", [
    (cascade, sql)
    for cascade in [True, False]
    for sql in [
        "CREATE SCHEMA s",
        "CREATE TABLE b (c DOUBLE PRECISION)",
        "CREATE TABLE s.b (c DOUBLE PRECISION)",
        "CREATE TYPE abc AS ENUM ('A', 'B', 'C')",
        "CREATE TYPE s.abc AS ENUM ('A', 'B', 'C')",
        "CREATE INDEX IF NOT EXISTS idx_a ON a (id)",
        "CREATE INDEX IF NOT EXISTS idx_a ON s.a (id)",
    ]
])
def test_drop_truncate(snapshot, cascade: bool, sql: str):
    codegen = list(drop(sql_as_operations(sql), cascade))
    snapshot.assert_match(pformat(codegen), "drop")

    codegen = list(truncate(sql_as_operations(sql), cascade))
    snapshot.assert_match(pformat(codegen), "truncate")


@pytest.mark.parametrize("sql", [
    *[
        sql % { 'src_ns': src_ns, 'rel_ns': rel_ns }
        for sql in [
            "CREATE TABLE %(src_ns)sa1 (a_id INT)",
            "CREATE TABLE %(src_ns)sa2 (" \
                    "a_id INT," \
                    "b_id INT," \
                    "FOREIGN KEY (b_id) REFERENCES %(rel_ns)sb(b_id))",
            "CREATE TABLE %(src_ns)sa3 (" \
                    "a_id INT," \
                    "b_id INT," \
                    "c_id INT," \
                    "FOREIGN KEY (b_id) REFERENCES %(rel_ns)sb(b_id)," \
                    "FOREIGN KEY (c_id) REFERENCES %(rel_ns)sc(c_id))",
        ]
        for src_ns in ['', 'a.']
        for rel_ns in ['', 'a.']
    ]
])
def test_create_table_add_fk(snapshot, sql: str):
    codegen = list(add_foreign_keys(sql_as_operations(sql)))
    snapshot.assert_match(pformat(codegen), "add_foreign_keys")

@pytest.mark.parametrize("sql,fks_for_table,allow,disallow", [
    (
        "CREATE TABLE a1 (a_id INT)",
        { (Stmt.CreateTable, 'a1'): { ('a_id', 'ameta', 'a_id'): 'fk_a1_ameta' } },
        [],
        ['fk_a1_ameta'],
    ),
    (
        "CREATE TABLE a2 (a_id INT, b_id INT, FOREIGN KEY (b_id) REFERENCES b(b_id))",
        { (Stmt.CreateTable, 'a2'): {('a_id', 'ameta', 'a_id'): 'fk_a2_ameta',
                                     ('b_id', 'b', 'b_id'): 'fk_a2_b' } },
        ['fk_a2_b'],
        ['fk_a2_ameta'],
    ),
    (
        "CREATE TABLE a3 (a_id INT, b_id INT, c_id INT," \
                "FOREIGN KEY (b_id) REFERENCES b(b_id)," \
                "FOREIGN KEY (c_id) REFERENCES c(c_id))",
        { (Stmt.CreateTable, 'a3'): {('a_id', 'ameta', 'a_id'): 'fk_a3_ameta',
                                     ('b_id', 'b', 'b_id'): 'fk_a3_b',
                                     ('c_id', 'c', 'c_id'): 'fk_a3_c' } },
        ['fk_a3_b', 'fk_a3_c'],
        ['fk_a3_ameta'],
    ),
    (
        "CREATE TABLE s.a4 (a_id INT, b_id INT, c_id INT," \
                "FOREIGN KEY (b_id) REFERENCES s.b(b_id)," \
                "FOREIGN KEY (c_id) REFERENCES s.c(c_id))",
        { (Stmt.CreateTable, 's.a4'): { ('a_id', 's.ameta', 'a_id'): 'fk_sa4_ameta',
                                        ('b_id', 's.b', 'b_id'): 'fk_sa4_b',
                                        ('c_id', 's.c', 'c_id'): 'fk_sa4_c' } },
        ['fk_sa4_b', 'fk_sa4_c'],
        ['fk_sa4_ameta'],
    ),
])
def test_create_table_rm_fk(
        snapshot, sql: str,
        fks_for_table: FkMap,
        allow: List[str],
        disallow: List[str],
):
    codegen = list(remove_foreign_keys(sql_as_operations(sql), fks_for_table))
    snapshot.assert_match(pformat(codegen), "remove_foreign_keys")
    assert not any([item in stmt for stmt in codegen for item in disallow])
    assert all([any([item in stmt for item in allow]) for stmt in codegen])

