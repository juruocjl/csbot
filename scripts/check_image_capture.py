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
import logging
from types import SimpleNamespace
from pyppeteer import launch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_runtime.image_archive import ArchiveImage, image_segment, MARKER

PAGE = '''<!doctype html><html><meta charset="utf-8"><body><div class="content">
<h1>游戏状态</h1><table><tr><th>群友</th><th>状态</th></tr><tr><td>合成甲</td><td>Counter-Strike 2</td></tr></table>
<img alt="合成甲" src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7">
<script>window.__CSBOT_PAGE_METADATA__={documentKey:location.pathname+location.search,pending:0,failed:false,responses:[{api:'/api/steam/status',params:{},data:{game:'Counter-Strike 2',not_rendered:77}}]};</script>
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
    async def full_data(key): return {'key': key, 'unrendered_analysis': 88}
    database = SimpleNamespace(**{name: full_data for name in ['get_match_detail','get_match_extra','get_match_gp_detail','get_match_gp_extra','get_match_faceit_detail','get_base_info','get_detail_info']})
    namespace = dict(launch=launch, asyncio=asyncio, LOCAL_URL=url, db_val=database, logger=logging.getLogger(__name__))
    try:
        page = function('plugins/cs_server/__init__.py', 'get_screenshot', namespace)
        html = function('plugins/utils/__init__.py', 'screenshot_html_to_png', namespace)
        images=[await page('/steam-status', 'synthetic-no-secret'), await html(url, 600, 300, archive_title='合成卡片', archive_snapshot={'rows':[{'game':'Counter-Strike 2','not_rendered':77}]})]
        for route in ['/match?id=fixture','/match-gp?id=fixture','/match-faceit?id=fixture','/data?steamId=fixture']:
            image = await page(route, 'synthetic-no-secret')
            assert 'unrendered_analysis' in str(image.metadata['snapshot']['analysis_data'])
            images.append(image)
        for image in images:
            assert isinstance(image, ArchiveImage), 'rendering lost metadata'
            assert bytes(image).startswith(b'\x89PNG')
            assert 'Counter-Strike 2' in str(image.metadata['snapshot'])
            assert 'not_rendered' in str(image.metadata['snapshot'])
            assert 'synthetic-no-secret' not in str(image.metadata)
            assert MARKER in image_segment(image).data
        assert isinstance(await html(url,600,300),bytes) and not isinstance(await html(url,600,300),ArchiveImage)
        print('PASS: production renderers preserve real PNG, full API/model metadata including unrendered fields, and exclude cookies/local paths; missing metadata keeps pixels')
    finally:
        server.shutdown(); server.server_close()

if __name__ == '__main__': asyncio.run(main())
