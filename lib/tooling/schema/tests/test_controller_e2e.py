import pytest
from ..type import Stmt, Transform

@pytest.mark.parametrize("t,sql_in,sql_out", [
    *[
        (cmd, sql, sql)
        for sql in [
            'ALTER TABLE a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES b(bb)',
            'ALTER TABLE a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES m.b(bb)',
            'ALTER TABLE n.a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES b(bb)',
            'ALTER TABLE n.a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES m.b(bb)',
        ]
        for cmd in [
            Transform.Create(omit_foreign_keys=False, run_raw_schema=False),
            Transform.AddForeignKeys(),
        ]
    ],
    *[
        (cmd, 'ALTER TABLE a ADD CONSTRAINT fk_1 FOREIGN KEY (aa) REFERENCES b(bb)', '')
        for cmd in [
            Transform.Create(omit_foreign_keys=True, run_raw_schema=False),
            [Transform.Drop(cascade) for cascade in [True, False]],
            [Transform.Truncate(cascade) for cascade in [True, False]],
            [Transform.ReIndex(set())],
            Transform.RemoveForeignKeys()
        ]
    ],
])
def test_alter_t_add_fk(t: Transform.T, sql_in: str, sql_out: str):
    pass

