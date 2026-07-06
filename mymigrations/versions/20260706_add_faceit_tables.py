"""add faceit tables

Revision ID: 20260706_add_faceit_tables
Revises:
Create Date: 2026-07-06
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260706_add_faceit_tables"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "steam_faceit_ids",
        sa.Column("steamid", sa.String(length=20), nullable=False),
        sa.Column("player_id", sa.String(length=50), nullable=False),
        sa.Column("nickname", sa.String(length=100), nullable=False),
        sa.Column("skill_level", sa.Integer(), nullable=False),
        sa.Column("faceit_elo", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("steamid"),
        sa.UniqueConstraint("player_id"),
    )
    op.create_table(
        "matches_faceit",
        sa.Column("mid", sa.String(length=60), nullable=False),
        sa.Column("player_id", sa.String(length=50), nullable=False),
        sa.Column("steamid", sa.String(length=20), nullable=True),
        sa.Column("nickname", sa.String(length=100), nullable=False),
        sa.Column("mapName", sa.String(length=50), nullable=False),
        sa.Column("team", sa.Integer(), nullable=False),
        sa.Column("winTeam", sa.Integer(), nullable=False),
        sa.Column("score1", sa.Integer(), nullable=False),
        sa.Column("score2", sa.Integer(), nullable=False),
        sa.Column("timeStamp", sa.BigInteger(), nullable=False),
        sa.Column("mode", sa.String(length=100), nullable=False),
        sa.Column("competitionName", sa.String(length=200), nullable=False),
        sa.Column("region", sa.String(length=20), nullable=False),
        sa.Column("kill", sa.Integer(), nullable=False),
        sa.Column("death", sa.Integer(), nullable=False),
        sa.Column("assist", sa.Integer(), nullable=False),
        sa.Column("adr", sa.Float(), nullable=False),
        sa.Column("rating", sa.Float(), nullable=False),
        sa.Column("kdRatio", sa.Float(), nullable=False),
        sa.Column("headshots", sa.Integer(), nullable=False),
        sa.Column("headshotsPct", sa.Integer(), nullable=False),
        sa.Column("mvp", sa.Integer(), nullable=False),
        sa.Column("skillLevel", sa.Integer(), nullable=False),
        sa.Column("faceitElo", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("mid", "player_id"),
    )


def downgrade() -> None:
    op.drop_table("matches_faceit")
    op.drop_table("steam_faceit_ids")
