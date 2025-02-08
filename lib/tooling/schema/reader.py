import re
from dataclasses import dataclass
from logging import getLogger
from os.path import relpath
from sqlglot import parse as parse_sql, Expression
import sqlglot.expressions as sql_expr
from typing import (
    cast,
    Iterator,
    Optional,
    Self,
    Type,
)

from lib.service.io import IoService
from lib.utility.concurrent import fmap
from lib.utility.iteration import partition

from .config import schema_ns
from .file_discovery import FileDiscovery, FileDiscoveryMatch
from .type import (
    AlterTableAction,
    Stmt,
    SchemaNamespace,
    SqlFileMetaData,
    SchemaSyntax,
    SchemaSteps,
    Ref,
)

class SchemaReader:
    logger = getLogger(f'{__name__}.SchemaReader')

    def __init__(self: Self,
                 file_discovery: FileDiscovery,
                 io: IoService) -> None:
        self.file_discovery = file_discovery
        self._io = io

    async def files(
        self: Self,
        name: SchemaNamespace,
        maybe_range: Optional[range] = None,
        load_syn=False,
    ) -> list[SqlFileMetaData]:
        metas = await self.file_discovery.ns_matches(name)

        return sorted([
            await self.__f_sql_meta_data(f, meta, load_syn)
            for f, meta in metas
            if maybe_range is None or meta.step in maybe_range
        ], key=lambda it: it.step)


    async def all_files(
            self: Self,
            names: Optional[set[SchemaNamespace]] = None,
            load_syn=False,
    ) -> SchemaSteps:
        return {
            namespace: sorted([
                await self.__f_sql_meta_data(f, meta, load_syn)
                for f, meta in [
                    (f, self.file_discovery.match_file(f))
                    for f in await self.file_discovery.ns_sql_files(namespace)
                ]
                for f in await self.file_discovery.ns_sql_files(namespace)
            ], key=lambda it: it.step)
            for namespace in (names or schema_ns)
        }

    async def __f_sql_meta_data(self: Self, f: str, meta: FileDiscoveryMatch, load_syn: bool) -> SqlFileMetaData:
        try:
            contents = await fmap(sql_as_operations, self._io.f_read(f)) if load_syn else None
            return SqlFileMetaData(f, self.file_discovery.root_dir, meta.ns, meta.step, meta.name, contents)
        except Exception as e:
            self.logger.error(f'failed on {f}')
            raise e

def sql_as_operations(file_data: str) -> SchemaSyntax:
    sql_exprs = parse_sql(file_data, read='postgres')
    generator_a = ((expr, expr_as_op(expr)) for expr in sql_exprs if expr)
    generator_b = [(expr, op) for expr, op in generator_a if op is not None]
    return SchemaSyntax(*partition(generator_b))

def get_identifiers(name_expr: Expression) -> Ref:
    from sqlglot.expressions import (
        Identifier as Id,
        Table as T,
        Schema as S,
        Dot,
    )
    match name_expr:
        case S(this=T(this=Id(this=name), db=schema)):
            return Ref(schema or None, name)
        case T(this=Id(this=name), args={'db': Id(this=schema) }):
            return Ref(schema, name)
        case T(this=Id(this=name)):
            return Ref(None, name)
        case Dot(this=Id(this=name), expression=Id(this=schema)):
            return Ref(schema or None, name)
    raise ValueError(f'unknown expression, {name}')

def is_create_partition(expr: sql_expr.Create) -> bool:
    if expr.args['properties'] is None:
        return False
    return any(
        isinstance(property, sql_expr.PartitionedOfProperty)
        for property in expr.args['properties'].expressions
    )

def get_prop(expr, Type):
    es = expr.args['properties'].expressions
    return next((e for e in es if isinstance(e, Type)))

def create_function(expr: Expression, command_e: str) -> Stmt.Op:
    match = re.search(r"FUNCTION\s+(?:(\w+)\.)?(\w+)\s*\(", command_e, re.IGNORECASE)
    if not match:
        raise TypeError(f'unknown {repr(expr)}')
    s_name = match.group(1) if match.group(1) else None
    t_name = match.group(2)
    return Stmt.CreateFunction(expr, Ref(s_name, t_name))

def get_alter_table_action(expr: Expression) -> AlterTableAction.Op:
    from sqlglot.expressions import (
        AddConstraint,
        Schema as S,
        Constraint as C,
        ForeignKey as Fk,
        Identifier as Id,
        Reference as R
    )
    match expr:
        case AddConstraint(expressions=[C(this=Id(this=fk_name), expressions=[
            Fk(expressions=[Id(this=col_name)], args={
                'reference': R(this=S(
                    this=_,
                    expressions=[Id(this=ref_col)],
                ) as ref_id_info),
                **kwargs
            }),
        ])]):
            ref_t = get_identifiers(ref_id_info)
            return AlterTableAction.ColumnAddForeignKey(expr, fk_name, col_name, ref_t, ref_col)
    raise TypeError(repr(expr))

def expr_as_op(expr: Expression) -> Optional[Stmt.Op]:
    match expr:
        case sql_expr.Create(kind="SCHEMA", this=schema_def):
            s_name = schema_def.db
            return Stmt.CreateSchema(expr, s_name)
        case sql_expr.Create(kind="VIEW", this=schema):
            materialized = any((
                p for p in expr.args['properties'].expressions
                if isinstance(p, sql_expr.MaterializedProperty)
            ))
            t_name = schema.this.this
            s_name = schema.db or None
            return Stmt.CreateView(expr, Ref(s_name, t_name), materialized)
        case sql_expr.Create(kind="TABLE", this=id_info) if is_create_partition(expr):
            return Stmt.CreateTablePartition(expr, get_identifiers(id_info))
        case sql_expr.Create(kind="TABLE", this=id_info):
            return Stmt.CreateTable(expr, get_identifiers(id_info))
        case sql_expr.Create(kind="INDEX", this=schema):
            t_name = schema.this.this
            return Stmt.CreateIndex(expr, t_name)
        case sql_expr.Create(kind="FUNCTION", this=id_info):
            return Stmt.CreateFunction(expr, get_identifiers(id_info.this))
        case sql_expr.Command(this="CREATE", expression=e):
            match re.findall(r'\w+', e.lower()):
                case ['function', *_]:
                    return create_function(expr, e)
                case ['or', 'replace', 'function', *_]:
                    return create_function(expr, e)
                case ['type', s_name, t_name, 'as', *_]:
                    return Stmt.CreateType(expr, Ref(s_name, t_name))
                case ['type', t_name, 'as', *_]:
                    return Stmt.CreateType(expr, Ref(None, t_name))
                case other:
                    print(repr(e))
                    raise TypeError(f'unknown command {repr(other)}')
        case sql_expr.Command(this="DO", expression=e):
            return Stmt.OpaqueDoBlock(expr)
        case sql_expr.Alter(kind="TABLE", this=id_info):
            actions = list(map(get_alter_table_action, expr.actions))
            table_r = get_identifiers(id_info)
            return Stmt.AlterTable(expr, table_r, actions)
        case sql_expr.Semicolon():
            return None
        case other:
            raise TypeError(f'unknown {repr(other)}')


