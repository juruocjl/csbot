"""add runtime config

Revision ID: 20260826_add_runtime_config
Revises: 20260706_add_faceit_tables
Create Date: 2026-08-26
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260826_add_runtime_config"
down_revision: Union[str, None] = "20260706_add_faceit_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    table = op.create_table(
        "runtime_config",
        sa.Column("key", sa.String(length=100), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.BigInteger(), nullable=False),
        sa.Column("updated_by", sa.String(length=20), nullable=True),
        sa.PrimaryKeyConstraint("key"),
    )
    op.bulk_insert(
        table,
        [
            {
                "key": "hltv_event_id_list",
                "value": "[]",
                "updated_at": 0,
                "updated_by": None,
            }
        ],
    )


def downgrade() -> None:
    op.drop_table("runtime_config")
