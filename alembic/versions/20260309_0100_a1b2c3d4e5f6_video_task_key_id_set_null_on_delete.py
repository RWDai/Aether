"""video_tasks.key_id: add ondelete SET NULL

Revision ID: a1b2c3d4e5f6
Revises: 2053ab8ed764
Create Date: 2026-03-09 01:00:00.000000+00:00

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "2053ab8ed764"
branch_labels = None
depends_on = None

_TABLE = "video_tasks"
_FK_NAME = "video_tasks_key_id_fkey"


def _fk_ondelete(table_name: str, constraint_name: str) -> str | None:
    bind = op.get_bind()
    result = bind.execute(
        sa.text(
            "SELECT rc.delete_rule "
            "FROM information_schema.referential_constraints rc "
            "JOIN information_schema.table_constraints tc "
            "  ON rc.constraint_name = tc.constraint_name "
            "WHERE tc.table_name = :table AND tc.constraint_name = :name"
        ),
        {"table": table_name, "name": constraint_name},
    )
    row = result.first()
    return row[0] if row else None


def _replace_fk_if_needed(
    constraint_name: str,
    table_name: str,
    ref_table: str,
    local_cols: list[str],
    remote_cols: list[str],
    desired_ondelete: str,
) -> None:
    current = _fk_ondelete(table_name, constraint_name)
    if current and current.upper() == desired_ondelete.upper():
        return
    if current:
        op.drop_constraint(constraint_name, table_name, type_="foreignkey")
    op.create_foreign_key(
        constraint_name,
        table_name,
        ref_table,
        local_cols,
        remote_cols,
        ondelete=desired_ondelete,
    )


def upgrade() -> None:
    _replace_fk_if_needed(
        _FK_NAME,
        _TABLE,
        "provider_api_keys",
        ["key_id"],
        ["id"],
        "SET NULL",
    )


def downgrade() -> None:
    _replace_fk_if_needed(
        _FK_NAME,
        _TABLE,
        "provider_api_keys",
        ["key_id"],
        ["id"],
        "NO ACTION",
    )
