"""Stable team identities, schedules, follows and durable notifications."""
from alembic import op
import sqlalchemy as sa

revision = "20261102_add_schedule"
down_revision = "20260826_add_runtime_config"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('schedule_delivery',
        sa.Column('match_id', sa.String(length=100), nullable=False, primary_key=True),
        sa.Column('group_id', sa.String(length=20), nullable=False, primary_key=True),
        sa.Column('revision', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('text', sa.Text(), nullable=False, primary_key=False),
        sa.Column('recipients', sa.Text(), nullable=False, primary_key=False),
        sa.Column('default_push', sa.Boolean(), nullable=False, primary_key=False),
        sa.Column('status', sa.String(length=20), nullable=False, primary_key=False),
        sa.Column('attempts', sa.Integer(), nullable=False, primary_key=False),
        sa.Column('next_attempt', sa.BigInteger(), nullable=False, primary_key=False),
        sa.Column('updated_at', sa.BigInteger(), nullable=False, primary_key=False),
    )
    op.create_index('ix_schedule_delivery_due', 'schedule_delivery', ['status', 'next_attempt'])
    op.create_index('ix_schedule_delivery_status', 'schedule_delivery', ['status'])
    op.create_table('schedule_event',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('payload', sa.Text(), nullable=False, primary_key=False),
        sa.Column('updated_at', sa.BigInteger(), nullable=False, primary_key=False),
    )
    op.create_table('schedule_match',
        sa.Column('id', sa.String(length=100), nullable=False, primary_key=True),
        sa.Column('event_id', sa.Integer(), nullable=False, primary_key=False),
        sa.Column('team1_id', sa.String(length=36), nullable=True, primary_key=False),
        sa.Column('team2_id', sa.String(length=36), nullable=True, primary_key=False),
        sa.Column('start_ts', sa.BigInteger(), nullable=False, primary_key=False),
        sa.Column('status', sa.String(length=20), nullable=False, primary_key=False),
        sa.Column('payload', sa.Text(), nullable=False, primary_key=False),
        sa.Column('revision', sa.Integer(), nullable=False, primary_key=False),
        sa.Column('updated_at', sa.BigInteger(), nullable=False, primary_key=False),
    )
    op.create_index('ix_schedule_match_event_id', 'schedule_match', ['event_id'])
    op.create_index('ix_schedule_match_start_ts', 'schedule_match', ['start_ts'])
    op.create_index('ix_schedule_match_status', 'schedule_match', ['status'])
    op.create_index('ix_schedule_match_team1_id', 'schedule_match', ['team1_id'])
    op.create_index('ix_schedule_match_team2_id', 'schedule_match', ['team2_id'])
    op.create_table('schedule_team',
        sa.Column('id', sa.String(length=36), nullable=False, primary_key=True),
        sa.Column('name', sa.String(length=150), nullable=False, primary_key=False),
        sa.Column('logo', sa.Text(), nullable=False, primary_key=False),
        sa.Column('discovery_at', sa.BigInteger(), nullable=False, primary_key=False),
    )
    op.create_table('schedule_follow',
        sa.Column('user_id', sa.String(length=20), nullable=False, primary_key=True),
        sa.Column('team_id', sa.String(length=36), sa.ForeignKey('schedule_team.id'), nullable=False, primary_key=True),
        sa.Column('created_at', sa.BigInteger(), nullable=False, primary_key=False),
    )
    op.create_table('schedule_notify_target',
        sa.Column('user_id', sa.String(length=20), nullable=False, primary_key=True),
        sa.Column('team_id', sa.String(length=36), sa.ForeignKey('schedule_team.id'), nullable=False, primary_key=True),
        sa.Column('group_id', sa.String(length=20), nullable=False, primary_key=True),
        sa.Column('created_at', sa.BigInteger(), nullable=False, primary_key=False),
    )
    op.create_table('schedule_team_alias',
        sa.Column('alias', sa.String(length=150), nullable=False, primary_key=True),
        sa.Column('team_id', sa.String(length=36), sa.ForeignKey('schedule_team.id'), nullable=False, primary_key=True),
    )
    op.create_table('schedule_team_source',
        sa.Column('source', sa.String(length=20), nullable=False, primary_key=True),
        sa.Column('external_id', sa.String(length=100), nullable=False, primary_key=True),
        sa.Column('team_id', sa.String(length=36), sa.ForeignKey('schedule_team.id'), nullable=False, primary_key=False),
    )
    op.create_index('ix_schedule_team_source_team_id', 'schedule_team_source', ['team_id'])


def downgrade():
    op.drop_table('schedule_team_source')
    op.drop_table('schedule_team_alias')
    op.drop_table('schedule_notify_target')
    op.drop_table('schedule_follow')
    op.drop_table('schedule_team')
    op.drop_table('schedule_match')
    op.drop_table('schedule_event')
    op.drop_table('schedule_delivery')
