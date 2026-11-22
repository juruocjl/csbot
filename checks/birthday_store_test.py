"""member_birthdays StateStore checks: temp SQLite only, no network/QQ.

Covers the schema, source semantics (qq_profile/manual/disabled/unknown),
manual-choice protection, name refresh, legacy ``birthday_overrides`` migration
and incremental lookup helpers used by the birthday plugin.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path
import types
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:  # POSIX-only dependency of the operational store; Windows checks stub it.
    import fcntl  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - Windows local checks only
    sys.modules['fcntl'] = types.SimpleNamespace(
        flock=lambda *args, **kwargs: None, LOCK_EX=2, LOCK_NB=4
    )

from ai_runtime.store import BIRTHDAY_SOURCES, StateStore, normalize_member_birthday


class MemberBirthdayStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(Path(self.tmp.name) / 'state.sqlite3')

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_table_schema_and_profile_row(self) -> None:
        columns = {row['name']: row for row in
                   self.store.rows('PRAGMA table_info(member_birthdays)')}
        self.assertEqual(set(columns), {'uid', 'mmdd', 'source', 'profile_year',
                                        'name', 'synced_at', 'updated_at'})
        self.assertIsNone(self.store.get_member_birthday('1001'))

        row = self.store.upsert_member_birthday(
            '1001', '7-10', 'qq_profile', profile_year=2000, name='小明')
        self.assertEqual((row['uid'], row['mmdd'], row['source']), ('1001', '07-10', 'qq_profile'))
        self.assertEqual(row['profile_year'], 2000)
        self.assertEqual(row['name'], '小明')
        self.assertGreater(row['synced_at'], 0)
        self.assertGreater(row['updated_at'], 0)
        self.assertEqual(self.store.get_member_birthday('1001')['mmdd'], '07-10')

    def test_source_semantics_and_validation(self) -> None:
        self.assertEqual(BIRTHDAY_SOURCES,
                         frozenset({'qq_profile', 'manual', 'disabled', 'unknown'}))
        self.assertEqual(normalize_member_birthday('2-9'), '02-09')
        self.assertIsNone(normalize_member_birthday('02-30'))

        with self.assertRaises(ValueError):
            self.store.upsert_member_birthday('1001', '07-10', 'nonsense')
        with self.assertRaises(ValueError):
            self.store.upsert_member_birthday('1001', None, 'manual')
        with self.assertRaises(ValueError):
            self.store.upsert_member_birthday('1001', '02-30', 'qq_profile')

        unknown = self.store.upsert_member_birthday('1002', '07-10', 'unknown')
        self.assertIsNone(unknown['mmdd'])
        disabled = self.store.upsert_member_birthday('1003', '07-10', 'disabled')
        self.assertIsNone(disabled['mmdd'])
        manual = self.store.upsert_member_birthday('1004', '07-10', 'manual')
        self.assertEqual(manual['synced_at'], 0)

    def test_manual_and_disabled_are_protected_unless_forced(self) -> None:
        self.store.upsert_member_birthday('1001', '07-10', 'manual')
        kept = self.store.upsert_member_birthday('1001', '08-11', 'qq_profile')
        self.assertEqual((kept['source'], kept['mmdd']), ('manual', '07-10'))

        forced = self.store.upsert_member_birthday('1001', '08-11', 'qq_profile', force=True)
        self.assertEqual((forced['source'], forced['mmdd']), ('qq_profile', '08-11'))

        self.store.upsert_member_birthday('1002', None, 'disabled')
        # An explicit manual edit may replace a disabled choice...
        changed = self.store.upsert_member_birthday('1002', '09-09', 'manual')
        self.assertEqual((changed['source'], changed['mmdd']), ('manual', '09-09'))
        # ...but a profile refresh never clobbers it.
        kept = self.store.upsert_member_birthday('1002', '10-10', 'qq_profile')
        self.assertEqual((kept['source'], kept['mmdd']), ('manual', '09-09'))

    def test_name_is_preserved_when_a_refresh_passes_none(self) -> None:
        self.store.upsert_member_birthday('1001', '07-10', 'qq_profile', name='旧名片')
        row = self.store.upsert_member_birthday('1001', '07-10', 'qq_profile', name=None)
        self.assertEqual(row['name'], '旧名片')
        self.assertEqual(self.store.update_member_birthday_names({'1001': '新名片'}), 1)
        self.assertEqual(self.store.get_member_birthday('1001')['name'], '新名片')
        # Same value again is a no-op, unknown members are ignored.
        self.assertEqual(self.store.update_member_birthday_names({'1001': '新名片'}), 0)
        self.assertEqual(self.store.update_member_birthday_names({'9999': '路人'}), 0)

    def test_list_member_birthdays_filters_and_returns_all(self) -> None:
        self.store.upsert_member_birthday('1001', '07-10', 'qq_profile')
        self.store.upsert_member_birthday('1002', None, 'unknown')
        self.store.upsert_member_birthday('1003', None, 'disabled')
        self.assertEqual(set(self.store.list_member_birthdays()),
                         {'1001', '1002', '1003'})
        self.assertEqual(set(self.store.list_member_birthdays(['1003', '1001', '404'])),
                         {'1001', '1003'})
        self.assertEqual(self.store.list_member_birthdays([]), {})

    def test_migrate_legacy_overrides_is_idempotent(self) -> None:
        migrated, remaining = self.store.migrate_birthday_overrides({
            '1001': '8-8', '1002': 'none', 'bad': '07-10', '1003': '02-30',
        })
        self.assertEqual(migrated, 2)
        self.assertEqual(set(remaining), {'bad', '1003'})
        row = self.store.get_member_birthday('1001')
        self.assertEqual((row['source'], row['mmdd']), ('manual', '08-08'))
        self.assertEqual(self.store.get_member_birthday('1002')['source'], 'disabled')

        # A newer manual row is never overwritten by a stale legacy entry.
        self.store.upsert_member_birthday('1001', '09-09', 'manual', force=True)
        migrated, _ = self.store.migrate_birthday_overrides({'1001': '08-08'})
        self.assertEqual(migrated, 0)
        self.assertEqual(self.store.get_member_birthday('1001')['mmdd'], '09-09')

        # Second run of the same legacy map changes nothing.
        migrated, _ = self.store.migrate_birthday_overrides({'1001': '08-08', '1002': 'none'})
        self.assertEqual(migrated, 0)

    def test_migration_overrides_profile_rows(self) -> None:
        self.store.upsert_member_birthday('1001', '07-10', 'qq_profile')
        self.store.upsert_member_birthday('1002', None, 'unknown')
        migrated, remaining = self.store.migrate_birthday_overrides(
            {'1001': '12-01', '1002': 'none'})
        self.assertEqual((migrated, remaining), (2, {}))
        self.assertEqual(self.store.get_member_birthday('1001')['source'], 'manual')
        self.assertEqual(self.store.get_member_birthday('1002')['source'], 'disabled')


if __name__ == '__main__':
    unittest.main()
