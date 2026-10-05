"""Semantic image storage contracts, no network or business database."""
import asyncio
import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.image_archive import (ArchiveImage, MARKER, snapshot_image, library_image,
    image_segment, validate_metadata, metadata_text, resource_info, page_source, capture_page_image, compact_metadata_text)


class Checks(unittest.TestCase):
    def test_snapshot_is_bounded_and_immutable(self):
        image = snapshot_image(b'pixels', '比分', {'score': '13:8'}, source={'id': 'match-1'})
        self.assertIsInstance(image, ArchiveImage)
        self.assertNotIn('13:8', metadata_text(image.metadata))
        self.assertIn('csdata.image', metadata_text(image.metadata))
        self.assertEqual(resource_info(image.metadata)['metadata']['snapshot']['score'], '13:8')
        segment = image_segment(image)
        self.assertEqual(segment.data[MARKER], image.metadata)
        info = resource_info(image.metadata)
        self.assertEqual(info['full_status'], 'metadata_only')
        self.assertIsNone(info['full_path'])
        modified = copy.deepcopy(image.metadata); modified['snapshot']['score'] = '0:0'
        with self.assertRaises(ValueError): validate_metadata(modified)
        self.assertNotIsInstance(snapshot_image(b'pixels', '大快照', {'text': 'x'*140000}), ArchiveImage)
        self.assertNotIsInstance(snapshot_image(b'pixels', '无快照', {}), ArchiveImage)

    def test_library_reference_never_follows_changed_or_symlink_source(self):
        old = Path.cwd()
        with tempfile.TemporaryDirectory() as tmp:
            try:
                os.chdir(tmp); path = Path('imgs/pic/a.png'); path.parent.mkdir(parents=True); path.write_bytes(b'fixture')
                image = library_image(path)
                self.assertEqual(resource_info(image.metadata)['full_status'], 'available')
                self.assertEqual(len(list(Path(tmp).rglob('*.*'))), 1)
                path.write_bytes(b'changed')
                self.assertEqual(resource_info(image.metadata)['full_status'], 'source_changed')
                path.unlink()
                self.assertEqual(resource_info(image.metadata)['full_status'], 'missing')
                outside = Path('secret'); outside.write_bytes(b'fixture'); path.symlink_to(outside.resolve())
                self.assertEqual(resource_info(image.metadata)['full_status'], 'missing')
                self.assertNotIsInstance(library_image(path), ArchiveImage)
            finally: os.chdir(old)

    def test_page_provenance_drops_unapproved_paths_and_parameters(self):
        self.assertEqual(page_source('/match?id=123&hideSidebar=True'), {'page': '/match', 'params': {'id': '123'}})
        for value in ['/admin/config', 'https://outside/match?id=1', '/match?token=SECRET', '/match?id=1&unknown=x']:
            self.assertIsNone(page_source(value))

    def test_capture_uses_stable_rendered_state_and_preserves_fallback(self):
        class Page:
            def __init__(self, values): self.values = iter(values)
            async def evaluate(self, *args): return next(self.values)
            async def screenshot(self, *args): return b'pixels'
        one = {'kind': 'business_api', 'responses': [{'api': '/api/steam/status', 'params': {}, 'data': {'full_unrendered_field': 'fixture'}}]}
        two = {'kind': 'business_api', 'responses': [{'api': '/api/steam/status', 'params': {}, 'data': {}}]}
        stable = asyncio.run(capture_page_image(Page([one, one]), title='游戏状态'))
        self.assertEqual(stable.metadata['snapshot'], one)
        for pair in ([one, two], [None, None]):
            self.assertNotIsInstance(asyncio.run(capture_page_image(Page(pair), title='游戏状态')), ArchiveImage)

    def test_legacy_indexed_metadata_becomes_a_reference_without_editing_archive(self):
        old = '[image:0123456789abcdef] 战绩（发送时数据快照，未归档图片像素；2026-10-05）'
        raw = old + '{"text":"千秋万代 21/6/2","image_labels":["千秋万代"]} 后续普通文字'
        projected = compact_metadata_text(raw)
        self.assertNotIn('21/6/2', projected)
        self.assertIn('csdata.image', projected)
        self.assertTrue(projected.endswith(' 后续普通文字'))
        self.assertIn('21/6/2', raw)
        self.assertNotIn('x'*100, compact_metadata_text(old + '{"text":"'+'x'*6000+'…[摘要截断，用 csdata.image 读取完整 metadata]'))

    def test_producer_models_preserve_all_columns_without_internal_orm_state(self):
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
        class Base(DeclarativeBase): pass
        class Player(Base):
            __tablename__ = 'fixture'
            id: Mapped[int] = mapped_column(primary_key=True)
            hidden_stat: Mapped[int]
        image = snapshot_image(b'pixels', '完整输入', {'player': Player(id=1, hidden_stat=77)})
        self.assertEqual(image.metadata['snapshot']['player'], {'id': 1, 'hidden_stat': 77})


if __name__ == '__main__': unittest.main()
