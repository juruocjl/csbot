"""Run the explicit migration and schedule flow in a rolled-back test-only schema.

Provide CS_DATABASE ending in /csbot_backup. Never prints the connection URL,
loads production configuration, touches public tables, or sends QQ messages.
"""
import asyncio
import copy
import importlib.util
import json
import os
from pathlib import Path
import uuid

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from schedule_test import ScheduleStore, event, match, parse_schedule_matches


async def main():
    url = make_url(os.environ['CS_DATABASE'])
    if url.database != 'csbot_backup' or url.get_backend_name() != 'postgresql':
        raise RuntimeError('Only PostgreSQL csbot_backup is permitted')
    schema = 'schedule_check_' + uuid.uuid4().hex
    engine = create_async_engine(url, connect_args={'timeout': 8})
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                assert await connection.scalar(text('SELECT current_database()')) == 'csbot_backup'
                await connection.execute(text(f'CREATE SCHEMA {schema}'))
                await connection.execute(text(f'SET LOCAL search_path TO {schema}'))
                def migrate(conn):
                    path = Path(__file__).parents[1]/'mymigrations/versions/20261102_add_schedule.py'
                    spec = importlib.util.spec_from_file_location('migration', path)
                    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
                    module.op = Operations(MigrationContext.configure(conn))
                    module.upgrade()
                await connection.run_sync(migrate)
                store = ScheduleStore(async_sessionmaker(bind=connection, expire_on_commit=False,
                                                        join_transaction_mode='create_savepoint'))
                info = event()
                await store.save_events([info])
                a = (await store.search('csgo_tm_1'))[0]['id']
                b = (await store.search('hltv_team_1'))[0]['id']
                assert a != b
                await store.follow('111', a, '100')
                await store.follow('222', b, '100')
                assert [e.event_id for e in (await store.watched([]))[0]] == [1]
                await store.ingest(info, parse_schedule_matches(1,[match()]), set(), [])
                assert (await store.schedule([1], '111'))['total'] == 1
                assert (await store.schedule([1], '333'))['total'] == 0
                for _ in range(2): await store.ingest(info, parse_schedule_matches(1,[match(status=1)]), set(), [])
                started = await store.claim_delivery(set(), {'100'})
                assert started['key'][2] == 0 and started['users'] == ['111','222']
                await store.delivery_result(started['key'], 'sent')
                for _ in range(2): await store.ingest(info, parse_schedule_matches(1,[match(status=2)]), set(), [])
                delivery = await store.claim_delivery(set(), {'100'})
                assert delivery['users'] == ['111','222']
                await store.delivery_result(delivery['key'], 'sent')
                assert await store.claim_delivery(set(), {'100'}) is None
                assert await connection.scalar(text('SELECT count(*) FROM schedule_delivery')) == 2
                print('PASS PostgreSQL: migration, opaque identities, all-user discovery, personal filtering, merged @ targets, start/result deduplication')
            finally:
                await transaction.rollback()
            assert await connection.scalar(text('SELECT to_regnamespace(:schema)'), {'schema':schema}) is None
            print('PASS rollback: temporary schema removed; public tables unchanged; no QQ sends')
    finally:
        await engine.dispose()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as exc:
        # Driver exceptions may contain connection details; keep diagnostics bounded.
        print('FAIL schedule PostgreSQL acceptance:', type(exc).__name__)
        raise SystemExit(1)
