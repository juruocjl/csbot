"""Check the source review manifest; --accept-reviewed updates hashes after review."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.source import FILES, REPO, MANIFEST, reviewed_files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--accept-reviewed', action='store_true', help='Only after reviewing all changed allowlisted files for credentials/private data')
    args = parser.parse_args()
    if args.accept_reviewed:
        manifest = {'version': 1, 'files': {name: hashlib.sha256((REPO/name).read_bytes()).hexdigest() for name in FILES}}
        (REPO/MANIFEST).write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    files = reviewed_files()
    print(f'PASS: {len(files)} reviewed source files, {sum(map(len, files.values()))} bytes; hashes and paths verified')


if __name__ == '__main__': main()
