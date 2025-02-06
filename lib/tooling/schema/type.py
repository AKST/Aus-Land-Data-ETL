from dataclasses import dataclass, field
from os.path import join as join_path
from sqlglot import Expression
from typing import Literal, Optional, Self

EntityKind = Literal['table', 'schema']

SchemaNamespace = Literal[
    'abs',
    'meta',
    'gnaf',
    'nsw_lrs',
    'nsw_planning',
    'nsw_spatial',
    'nsw_gnb',
    'nsw_vg',
]

class Command:
    @dataclass
    class BaseCommand:
        ns: SchemaNamespace
        range: Optional[range] = field(default=None)
        dryrun: bool = field(default=False)

    @dataclass
    class Create(BaseCommand):
        omit_foreign_keys: bool = field(default=False)
        run_raw_schema: bool = field(default=False)

    @dataclass
    class ReIndex(BaseCommand):
        allowed: set[EntityKind] = field(default_factory=lambda: set())

    @dataclass
    class AddForeignKeys(BaseCommand):
        pass

    @dataclass
    class RemoveForeignKeys(BaseCommand):
        pass

    @dataclass
    class Drop(BaseCommand):
        cascade: bool = field(default=False)

    @dataclass
    class Truncate(BaseCommand):
        cascade: bool = field(default=False)

@dataclass
class Ref:
    schema_name: Optional[str]
    name: str

    def __str__(self: Self) -> str:
        match self.schema_name:
            case None: return self.name
            case schema: return f'{schema}.{self.name}'

class Stmt:
    @dataclass
    class Op:
        expr_tree: Expression = field(repr=False)

    @dataclass
    class OpaqueDoBlock(Op):
        pass

    @dataclass
    class CreateSchema(Op):
        schema_name: str

    @dataclass
    class CreateTable(Op):
        table: Ref

    @dataclass
    class CreateTablePartition(Op):
        partition: Ref

    @dataclass
    class CreateView(Op):
        view: Ref
        materialized: bool

    @dataclass
    class CreateType(Op):
        type: Ref

    @dataclass
    class CreateFunction(Op):
        func: Ref

    @dataclass
    class CreateIndex(Op):
        index_name: str

        @property
        def is_concurrent(self: Self) -> bool:
            return self.expr_tree.args['concurrently']

@dataclass
class SchemaSyntax:
    expr_tree: list[Expression] = field(repr=False)
    operations: list[Stmt.Op]

    @property
    def can_be_used_in_transaction(self: Self) -> bool:
        return not any(
            operation.is_concurrent
            for operation in self.operations
            if isinstance(operation, Stmt.CreateIndex)
        )

@dataclass
class SqlFileMetaData:
    file_name: str
    root_dir: str
    ns: SchemaNamespace
    step: int
    name: Optional[str]
    contents: Optional[SchemaSyntax]

    @property
    def is_known_to_be_transaction_unsafe(self: Self) -> bool:
        match self.contents:
            case None:
                return False
            case contents:
                return not self.contents.can_be_used_in_transaction

    def path(self: Self) -> str:
        step_s = f'{self.step:03}'
        suffix = f'_{self.name}' if self.name else f''
        f_name = f'{step_s}_APPLY{suffix}.sql'
        return join_path(self.root_dir, self.ns, 'schema', f_name)

SchemaSteps = dict[SchemaNamespace, list[SqlFileMetaData]]
