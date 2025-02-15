import pytest
from unittest.mock import AsyncMock

from lib.service.database.mock import MockDatabaseService
from lib.service.io import IoService
from lib.service.uuid.mocks import MockUuidService

from ..controller import SchemaController
from ..reader import SchemaReader, sql_as_operations
from ..type import Command, Stmt, Transform, SqlFileMetaData

@pytest.mark.asyncio
@pytest.mark.parametrize("t,sql_in,sql_out", [
    *[
        (cmd, sql, sql)
        for sql in [
            'ALTER TABLE a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES b(bb);',
            'ALTER TABLE a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES m.b(bb);',
            'ALTER TABLE n.a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES b(bb);',
            'ALTER TABLE n.a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES m.b(bb);',
        ]
        for cmd in [
            # Transform.Create(omit_foreign_keys=False, run_raw_schema=False),
            Transform.AddForeignKeys(),
        ]
    ],
    *[
        (cmd, 'ALTER TABLE a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES b (bb);', '')
        for cmd in [
            Transform.Create(omit_foreign_keys=True, run_raw_schema=False),
            *[Transform.Drop(cascade) for cascade in [True, False]],
            *[Transform.Truncate(cascade) for cascade in [True, False]],
            Transform.ReIndex(set()),
            Transform.RemoveForeignKeys()
        ]
    ],
    *[
        (
            Transform.Create(omit_foreign_keys=True, run_raw_schema=False),
            f'ALTER TABLE {n} SET SCHEMA blah;',
            f'ALTER TABLE {n} SET SCHEMA blah;',
        )
        for n in ['a', 'a.a']
    ],
])
async def test_alter_t_add_fk(t: Transform.T, sql_in: str, sql_out: str):
    db = MockDatabaseService()
    io = AsyncMock(spec=IoService)
    reader = AsyncMock(spec=SchemaReader)
    reader.files.return_value = [
        SqlFileMetaData(
            file_name='mock_file',
            root_dir='mock_root',
            ns='abs',
            step=1,
            name=None,
            contents=sql_as_operations(sql_in, lambda: 'mock-uuid'),
        ),
    ]
    ctrl = SchemaController(io, db, reader)
    await ctrl.command(Command('abs', None, False, t))
    assert ''.join(s for s, _ in db.state.execute_args) == sql_out

