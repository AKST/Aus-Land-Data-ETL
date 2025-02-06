from collections import namedtuple
from sqlglot import expressions, Expression
from typing import Dict, Iterator, List, Optional, Set, Tuple, Type

from lib.service.database import DbCursorLike
from ..type import Ref, Stmt, SchemaSyntax, EntityKind

def create(commands: SchemaSyntax, omit_foreign_keys: bool) -> Iterator[str]:
    for operation in commands.operations:
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                yield expr.sql(dialect='postgres')
            case Stmt.CreateType(expr, Ref(schema_name, name)):
                yield expr.sql(dialect='postgres')
            case Stmt.CreateTable(expr, Ref(schema_name, name)):
                copy = expr.copy()
                if omit_foreign_keys:
                    copy.this.set('expressions', [
                        sub_expr
                        for sub_expr in copy.this.expressions
                        if not isinstance(sub_expr, expressions.ForeignKey)
                    ])
                yield copy.sql(dialect='postgres')
            case Stmt.CreateTablePartition(expr, Ref(schema_name, name)):
                yield expr.sql(dialect='postgres')
            case Stmt.CreateFunction(expr, Ref(schema_name, name)):
                yield expr.sql(dialect='postgres')
            case Stmt.CreateIndex(expr, name):
                yield expr.sql(dialect='postgres')
            case Stmt.CreateView(expr, Ref(schema_name, name)):
                yield expr.sql(dialect='postgres')
            case Stmt.OpaqueDoBlock(expr):
                yield expr.sql(dialect='postgres')
            case other:
                raise TypeError(f'have not handled {other}')

def drop(commands: SchemaSyntax, cascade: bool = False) -> Iterator[str]:
    sfx = ' CASCADE' if cascade else ''

    for operation in reversed(commands.operations):
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                yield f'DROP SCHEMA IF EXISTS {schema_name}{sfx}'
            case Stmt.CreateType(expr, ref):
                yield f'DROP TYPE IF EXISTS {str(ref)}{sfx}'
            case Stmt.CreateTable(expr, ref):
                yield f'DROP TABLE IF EXISTS {str(ref)}{sfx}'
            case Stmt.CreateFunction(expr, ref):
                yield f'DROP FUNCTION IF EXISTS {str(ref)}{sfx}'
            case Stmt.CreateTablePartition(expr, ref):
                yield f'DROP TABLE IF EXISTS {str(ref)}{sfx}'
            case Stmt.CreateIndex(expr, name):
                yield f'DROP INDEX IF EXISTS {name}'
            case Stmt.CreateView(expr, ref, materialized):
                kind = 'MATERIALIZED VIEW' if materialized else 'view'
                yield f'DROP {kind} IF EXISTS {str(ref)}{sfx}'
            case Stmt.OpaqueDoBlock(expr):
                continue
            case other:
                raise TypeError(f'have not handled {other}')

def truncate(commands: SchemaSyntax, cascade: bool = False) -> Iterator[str]:
    sfx = ' CASCADE' if cascade else ''

    for operation in reversed(commands.operations):
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                continue
            case Stmt.CreateType(expr, ref):
                continue
            case Stmt.CreateTable(expr, ref):
                yield f'TRUNCATE TABLE {str(ref)}{sfx}'
            case Stmt.CreateTablePartition(expr, ref):
                yield f'TRUNCATE TABLE {str(ref)}{sfx}'
            case Stmt.CreateFunction(expr, ref):
                continue
            case Stmt.CreateIndex(expr, name):
                continue
            case Stmt.CreateView(expr, ref, materialized):
                continue
            case Stmt.OpaqueDoBlock(expr):
                continue
            case other:
                raise TypeError(f'have not handled {other}')

def add_foreign_keys(contents: SchemaSyntax) -> Iterator[str]:
    for operation in contents.operations:
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                continue
            case Stmt.CreateType(expr, ref):
                continue
            case Stmt.CreateIndex(expr, name):
                continue
            case Stmt.CreateTable(expr, ref):
                for col, rel, rel_col in _table_foreign_keys(expr):
                    yield f"ALTER TABLE {
                        str(rel)
                    } ADD CONSTRAINT fk_{
                        col
                    } FOREIGN KEY ({col}) REFERENCES {rel}({rel_col});"
            case Stmt.CreateTablePartition(expr, ref):
                continue
            case Stmt.CreateFunction(expr, ref):
                continue
            case Stmt.CreateView(expr, ref, materialized):
                continue
            case Stmt.OpaqueDoBlock(expr):
                continue
            case other:
                raise TypeError(f'have not handled {other}')

def reindex(commands: SchemaSyntax, allowed: Set[EntityKind]):
    for operation in reversed(commands.operations):
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                if 'schema' in allowed:
                    yield f'REINDEX SCHEMA {schema_name}'
            case Stmt.CreateType(expr, ref):
                continue
            case Stmt.CreateTable(expr, ref):
                if 'table' in allowed:
                    yield f'REINDEX TABLE {str(ref)}'
            case Stmt.CreateTablePartition(expr, ref):
                continue
            case Stmt.CreateFunction(expr, ref):
                continue
            case Stmt.CreateIndex(expr, name):
                continue
            case Stmt.CreateView(expr, ref, materialized):
                continue
            case Stmt.OpaqueDoBlock(expr):
                continue
            case other:
                raise TypeError(f'have not handled {other}')

FkDefinition = Tuple[str, str, str]
FkMap = Dict[Tuple[Type[Stmt.Op], str], Optional[Dict[FkDefinition, str]]]

async def make_fk_map(contents: SchemaSyntax, cursor: DbCursorLike) -> FkMap:
    query_without_ns = """
        SELECT tc.constraint_name,
               kcu.column_name,
               CASE
                   WHEN ccu.table_schema IS NOT NULL AND ccu.table_schema <> 'public'
                   THEN ccu.table_schema || '.' || ccu.table_name
                   ELSE ccu.table_name
               END,
               ccu.column_name
          FROM information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu USING (constraint_name)
          JOIN information_schema.constraint_column_usage AS ccu USING (constraint_name)
         WHERE tc.constraint_type = 'FOREIGN KEY'
           AND tc.table_name = %s
    """
    query_with_ns = f"{query_without_ns} AND tc.table_schema = %s"

    out: FkMap = {}
    for operation in contents.operations:
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                out[(Stmt.CreateSchema, schema_name)] = None
            case Stmt.CreateType(expr, ref):
                out[(Stmt.CreateType, str(ref))] = None
            case Stmt.CreateIndex(expr, name):
                out[(Stmt.CreateIndex, name)] = None
            case Stmt.CreateTable(expr, Ref(schema_name, name)):
                args: Tuple[str] | Tuple[str, str]
                if schema_name:
                    query = query_with_ns
                    args = (name, schema_name)
                else:
                    query = query_without_ns
                    args = (name,)
                await cursor.execute(query, args)
                out[(Stmt.CreateTable, str(ref))] = {
                    (col, rel, rel_col): name
                    for name, col, rel, rel_col in await cursor.fetchall()
                }
                result = await cursor.fetchall()
            case Stmt.CreateTablePartition(expr, ref):
                out[(Stmt.CreateTablePartition, str(ref))] = None
            case Stmt.CreateFunction(expr, ref):
                out[(Stmt.CreateFunction, str(ref))] = None
            case Stmt.CreateView(expr, ref, materialized):
                out[(Stmt.CreateView, str(ref))] = None
            case Stmt.OpaqueDoBlock(expr):
                continue
            case other:
                raise TypeError(f'have not handled {other}')
    return out

def remove_foreign_keys(contents: SchemaSyntax, table_fks: FkMap) -> Iterator[str]:
    for operation in reversed(contents.operations):
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                continue
            case Stmt.CreateType(expr, ref):
                continue
            case Stmt.CreateIndex(expr, name):
                continue
            case Stmt.CreateTable(expr, ref):
                t_name = str(ref)
                t_fkmap = table_fks[(Stmt.CreateTable, t_name)]
                yield from [
                    f"ALTER TABLE {t_name} DROP CONSTRAINT IF EXISTS {t_fkmap[fk]};"
                    for fk in _table_foreign_keys(expr)
                    if t_fkmap and fk in t_fkmap
                ]
            case Stmt.CreateTablePartition(expr, ref):
                continue
            case Stmt.CreateView(expr, ref, materialized):
                continue
            case Stmt.OpaqueDoBlock(expr):
                continue
            case other:
                raise TypeError(f'have not handled {other}')

def _table_foreign_keys(expr: Expression) -> Iterator[Tuple[str, str, str]]:
    for fk in expr.find_all(expressions.ForeignKey):
        col = fk.expressions[0].sql()
        rel = fk.args['reference'].this.this.sql()
        rel_col = fk.args['reference'].this.expressions[0].sql()
        yield (col, rel, rel_col)
