"""Bounded original-image cache. Metadata and thumbnails survive eviction.

The database is an operational index, not the authority for file existence.
Only cache-owned hash paths are eligible for deletion. Existing originals use
mtime as their initial recency; subsequent admissions/reads use explicit LRU.
"""
from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path

from PIL import Image

HASH = re.compile(r"^[a-f0-9]{64}$")


class MediaCache:
    def __init__(self, root: Path, budget: int = 1024**3, private_root: Path | None = None):
        if budget <= 0:
            raise ValueError("image budget must be positive")
        self.root = root.resolve()
        self.full = self.root / "full"
        self.small = self.root / "small"
        self.private_root=(private_root or self.root/'.private').resolve()
        self.private_full=self.private_root/'full'
        self.private_small=self.private_root/'small'
        self.budget = budget
        self.lock = threading.RLock()
        self.pins: dict[str, int] = {}
        for directory in (self.root, self.full, self.small):
            directory.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / "media-cache.sqlite3", check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("""CREATE TABLE IF NOT EXISTS media (
            hash TEXT PRIMARY KEY, size INTEGER NOT NULL, last_access REAL NOT NULL,
            status TEXT NOT NULL, width INTEGER, height INTEGER, mime TEXT,
            evicted_at REAL)""")
        if 'private' not in {r[1] for r in self.db.execute('PRAGMA table_info(media)')}:
            self.db.execute('ALTER TABLE media ADD COLUMN private INTEGER NOT NULL DEFAULT 0')
        self.db.commit()

    def reconcile(self, *, evict: bool = True) -> dict:
        """Adopt legacy files without reading all image bytes into memory."""
        with self.lock:
            for path in self.full.iterdir():
                if path.suffix != ".png" or not HASH.fullmatch(path.stem) or path.is_symlink():
                    continue
                stat = path.stat()
                self.db.execute("""INSERT INTO media(hash,size,last_access,status)
                    VALUES(?,?,?,'available') ON CONFLICT(hash) DO UPDATE
                    SET size=excluded.size,status='available'""",
                    (path.stem, stat.st_size, stat.st_mtime))
            self.db.commit()
            return self.prune() if evict else self.usage()

    def usage(self) -> dict:
        with self.lock:
            count, size = self.db.execute(
                "SELECT count(*),coalesce(sum(size),0) FROM media WHERE status='available'"
            ).fetchone()
            return {"original_count": count, "original_bytes": size, "budget_bytes": self.budget}

    def migrate_downloads(self, directory: Path, registered: dict) -> dict:
        """Adopt only filenames recorded by the old downloader, then remove duplicates."""
        remaining=dict(registered)
        for name in registered:
            if not isinstance(name,str) or Path(name).name!=name: continue
            path=directory/name
            if path.is_symlink(): continue
            if not path.exists():
                remaining.pop(name,None)
                continue
            if not path.is_file() or path.stat().st_size>32*1024**2: continue
            try:
                self.put(path.read_bytes())  # Thumbnail and metadata precede deletion.
            except (OSError,ValueError):
                continue
            path.unlink()
            remaining.pop(name,None)
        return remaining

    def put(self, content: bytes, *, private=False) -> str:
        digest = hashlib.sha256((b'private-image\0' if private else b'')+content).hexdigest()
        with self.lock:
            full_dir,small_dir=(self.private_full,self.private_small) if private else (self.full,self.small)
            if private:
                self.private_root.mkdir(parents=True,exist_ok=True,mode=0o700)
                self.private_root.chmod(0o700)
                for directory in (full_dir,small_dir):directory.mkdir(exist_ok=True,mode=0o700)
            with Image.open(BytesIO(content)) as image:
                width, height = image.size
                if width * height > 64_000_000:
                    raise ValueError("image exceeds pixel limit")
                mime = Image.MIME.get(image.format or "", "application/octet-stream")
                thumb_path = small_dir / f"{digest}.png"
                if not thumb_path.is_file():
                    image.seek(0)
                    thumb = image.convert("RGBA")
                    thumb.thumbnail((256, 256))
                    tmp = thumb_path.with_suffix(".tmp")
                    thumb.save(tmp, format="PNG")
                    os.replace(tmp, thumb_path)
            # The legacy .png suffix is retained; bytes may be JPEG/GIF/WebP.
            full_path = full_dir / f"{digest}.png"
            if not full_path.is_file():
                tmp = full_path.with_suffix(".tmp")
                tmp.write_bytes(content)
                os.replace(tmp, full_path)
            self.db.execute("""INSERT INTO media
                (hash,size,last_access,status,width,height,mime,evicted_at,private)
                VALUES(?,?,?,'available',?,?,?,NULL,?) ON CONFLICT(hash) DO UPDATE SET
                size=excluded.size,last_access=excluded.last_access,status='available',
                width=excluded.width,height=excluded.height,mime=excluded.mime,evicted_at=NULL""",
                (digest, len(content), time.time(), width, height, mime,int(private)))
            self.db.commit()
            self.prune()
            return digest

    def info(self, digest: str) -> dict:
        if not HASH.fullmatch(digest):
            raise ValueError("full image hash required")
        with self.lock:
            row = self.db.execute("SELECT status,width,height,mime,size,private FROM media WHERE hash=?", (digest,)).fetchone()
            full = (self.private_full if row and row[5] else self.full) / f"{digest}.png"
            small = (self.private_small if row and row[5] else self.small) / f"{digest}.png"
            exists = full.is_file() and not full.is_symlink()
            state = "available" if exists else (row[0] if row and row[0] == "evicted" else "missing")
            if row and not exists and row[0] == "available":
                self.db.execute("UPDATE media SET status='missing' WHERE hash=?", (digest,))
                self.db.commit()
            return {"image_id": digest, "full_status": state,
                    "full_path": str(full) if exists else None,
                    "thumbnail_path": str(small) if small.is_file() and not small.is_symlink() else None,
                    "width": row[1] if row else None, "height": row[2] if row else None,
                    "mime": row[3] if row else None, "size_bytes": row[4] if row else None}

    @contextmanager
    def lease(self, digest: str):
        """A consumer must hold the lease until it has finished copying/reading."""
        with self.lock:
            info = self.info(digest)
            if not info["full_path"]:
                raise FileNotFoundError(f"image {digest}: {info['full_status']}")
            self.pins[digest] = self.pins.get(digest, 0) + 1
            self.db.execute("UPDATE media SET last_access=? WHERE hash=?", (time.time(), digest))
            self.db.commit()
        try:
            yield Path(info["full_path"])
        finally:
            with self.lock:
                self.pins[digest] -= 1
                if not self.pins[digest]:
                    del self.pins[digest]
                self.prune()

    def prune(self) -> dict:
        with self.lock:
            total = self.usage()["original_bytes"]
            if total <= self.budget:
                return self.usage()
            target = int(self.budget * .9)
            # Iterate metadata, never read originals while collecting candidates.
            for digest, size, private in self.db.execute(
                "SELECT hash,size,private FROM media WHERE status='available' ORDER BY last_access,hash"
            ).fetchall():
                if total <= target:
                    break
                if self.pins.get(digest):
                    continue
                path = (self.private_full if private else self.full) / f"{digest}.png"
                if path.is_symlink():
                    continue
                path.unlink(missing_ok=True)
                self.db.execute("UPDATE media SET status='evicted',evicted_at=? WHERE hash=?", (time.time(), digest))
                total -= size
            self.db.commit()
            result = self.usage()
            result["over_budget_pinned"] = total > self.budget
            return result


_cache: MediaCache | None = None


def media_cache() -> MediaCache:
    global _cache
    if _cache is None:
        _cache = MediaCache(Path(os.getenv("CS_IMAGE_HISTORY_DIR", "imgs/history")),
                            int(os.getenv("CS_IMAGE_FULL_BUDGET_BYTES", str(1024**3))),
                            Path(os.getenv('CS_AI_STATE_DIR','data/ai'))/'media')
    return _cache
