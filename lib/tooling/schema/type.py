from abc import ABC, abstractmethod
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

class Transform:
    @dataclass
    class T(ABC):
        ...

    @dataclass
    class Create(T):
        omit_foreign_keys: bool
        run_raw_schema: bool

    @dataclass
    class ReIndex(T):
        allowed: set[EntityKind]

    @dataclass
    class AddForeignKeys(T):
        pass

    @dataclass
    class RemoveForeignKeys(T):
        pass

    @dataclass
    class Drop(T):
        cascade: bool

    @dataclass
    class Truncate(T):
        cascade: bool

@dataclass
class Command:
    ns: SchemaNamespace
    ns_range: Optional[range]
    dryrun: bool
    transform: Transform.T

    @staticmethod
    def create(ns: SchemaNamespace, ns_range: Optional[range] = None, dryrun: bool = False,
               omit_foreign_keys: bool = False, run_raw_schema: bool = False):
        return Command(ns, ns_range, dryrun, Transform.Create(omit_foreign_keys, run_raw_schema))

    @staticmethod
    def reindex(ns: SchemaNamespace, ns_range: Optional[range] = None, dryrun: bool = False,
                allowed: Optional[set[EntityKind]] = None):
        return Command(ns, ns_range, dryrun, Transform.ReIndex(allowed or set()))

    @staticmethod
    def drop(ns: SchemaNamespace, ns_range: Optional[range] = None, dryrun: bool = False,
             cascade: bool = False):
        return Command(ns, ns_range, dryrun, Transform.Drop(cascade))

    @staticmethod
    def truncate(ns: SchemaNamespace, ns_range: Optional[range] = None, dryrun: bool = False,
                 cascade: bool = False):
        return Command(ns, ns_range, dryrun, Transform.Truncate(cascade))

    @staticmethod
    def add_fk(ns: SchemaNamespace, ns_range: Optional[range] = None, dryrun: bool = False):
        return Command(ns, ns_range, dryrun, Transform.AddForeignKeys())

    @staticmethod
    def rm_fk(ns: SchemaNamespace, ns_range: Optional[range] = None, dryrun: bool = False):
        return Command(ns, ns_range, dryrun, Transform.RemoveForeignKeys())

@dataclass
class Ref:
    schema_name: Optional[str]
    name: str

    def __str__(self: Self) -> str:
        match self.schema_name:
            case None: return self.name
            case schema: return f'{schema}.{self.name}'

@dataclass
class OptionalName:
    @dataclass
    class T(ABC):
        ...

        @abstractmethod
        def __str__(self) -> str:
            ...

    @dataclass
    class Static(T):
        name: str

        def __str__(self: Self) -> str:
            return f'static:{self.name}'

    @dataclass
    class Anon(T):
        id: str
        hydrated_name: str | None

        def __str__(self: Self) -> str:
            return f'Anon:{self.id}'

class Stmt:
    @dataclass
    class Op(ABC):
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
        index_name: OptionalName.T

        @property
        def is_concurrent(self: Self) -> bool:
            return self.expr_tree.args['concurrently']

    @dataclass
    class AlterTable(Op, ABC):
        altered_table: Ref
        actions: list['AlterTableAction.Op']

class AlterTableAction:
    @dataclass
    class Op(ABC):
        expr_tree: Optional[Expression] = field(repr=False)

    @dataclass
    class SetSchema(Op):
        schema: str

    @dataclass
    class ColumnConstraint(Op, ABC):
        constraint_name: str
        constraint_column: str

    @dataclass
    class ColumnAddForeignKey(ColumnConstraint):
        ref_table: Ref
        ref_column: str

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
