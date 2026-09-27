"""Source publication boundaries: no bot/network/database required."""
from pathlib import Path
import hashlib
import json
import shutil
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.source import FILES, MANIFEST, REPO, reviewed_files, source_snapshot


class Checks(unittest.TestCase):
    def test_exact_snapshot_index_and_cleanup(self):
        with source_snapshot() as view:
            root = view.root
            self.assertIn('git commit:', view.guide)
            for name, content in reviewed_files().items():
                self.assertEqual((root/name).read_bytes(), content)
            index = (root/'plugins/fudu/__init__.index.md').read_text()
            self.assertIn('calc_roll_point', index)
            self.assertIn('MessageClass.add_uid', index)
            self.assertEqual({p.relative_to(root).as_posix() for p in root.rglob('*.py')}, set(FILES))
            self.assertFalse((root/'.git').exists())
            self.assertFalse((root/'.env.prod').exists())
            self.assertFalse((root/'data').exists())
            self.assertEqual((root/'plugins/fudu/__init__.py').stat().st_mode & 0o222, 0)
        self.assertFalse(root.exists())

    def test_modified_code_and_symlinks_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            for name in [MANIFEST, *FILES]:
                (repo/name).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(REPO/name, repo/name)
            name = next(iter(FILES)); path = repo/name
            original = path.read_bytes()
            path.write_bytes(original + b'\n# changed after review\n')
            with self.assertRaisesRegex(ValueError, 'changed since review'): reviewed_files(repo)
            path.unlink()
            outside = repo/'not-reviewed.py'; outside.write_bytes(original)
            path.symlink_to(outside)
            with self.assertRaisesRegex(ValueError, 'unavailable'): reviewed_files(repo)
            path.unlink(); path.write_bytes(original)
            manifest = json.loads((repo/MANIFEST).read_text())
            manifest['files']['.env.prod'] = hashlib.sha256(b'secret').hexdigest()
            (repo/MANIFEST).write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'allowlist'): reviewed_files(repo)


if __name__ == '__main__': unittest.main()
