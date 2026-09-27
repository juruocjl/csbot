"""Linux headless rendering acceptance, synthetic page only; no DB or QQ.

Exercises both production screenshot functions by extracting their actual AST,
without starting plugins, schedulers or loading credentials.
"""
import ast
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
from pyppeteer import launch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.image_archive import ArchiveImage, image_segment, MARKER

PAGE = '''<!doctype html><html><meta charset="utf-8"><body><div class="content">
<h1>游戏状态</h1><table><tr><th>群友</th><th>状态</th></tr><tr><td>合成甲</td><td>Counter-Strike 2</td></tr></table>
<img alt="合成甲" src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7">
</div></body></html>'''
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self):
        self.send_response(200); self.send_header('Content-Type', 'text/html; charset=utf-8'); self.end_headers(); self.wfile.write(PAGE.encode())


def function(path, name, namespace):
    source = Path(__file__).resolve().parents[1] / path
    node = next(n for n in ast.parse(source.read_text()).body if isinstance(n, ast.AsyncFunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), namespace)
    return namespace[name]


async def main():
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{server.server_port}'
    namespace = dict(launch=launch, asyncio=asyncio, LOCAL_URL=url)
    try:
        page = function('plugins/cs_server/__init__.py', 'get_screenshot', namespace)
        html = function('plugins/utils/__init__.py', 'screenshot_html_to_png', namespace)
        for image in [await page('/steam-status', 'synthetic-no-secret'), await html(url, 600, 300, archive_title='合成卡片')]:
            assert isinstance(image, ArchiveImage), 'rendering lost metadata'
            assert bytes(image).startswith(b'\x89PNG')
            assert 'Counter-Strike 2' in image.metadata['snapshot']['text']
            assert image.metadata['snapshot']['image_labels'] == ['合成甲']
            assert 'synthetic-no-secret' not in str(image.metadata)
            assert MARKER in image_segment(image).data
        print('PASS: real webpage and HTML renderers preserve PNG for sending, archive rendered text/labels, and exclude cookies/local paths')
    finally:
        server.shutdown(); server.server_close()

if __name__ == '__main__': asyncio.run(main())
