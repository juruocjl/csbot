#!/usr/bin/env python3
"""Preview production images locally without syncing them to disk."""

from __future__ import annotations

import argparse
import html
import json
import re
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import PurePosixPath
from urllib.parse import quote, unquote, urljoin, urlsplit
from urllib.request import Request, urlopen


SUPPORTED_EXTENSIONS = {".avif", ".bmp", ".gif", ".jpeg", ".jpg", ".png", ".svg", ".webp"}
ENTRY_PATTERN = re.compile(
    r'<a\s+href="([^"]+)">.*?</a>\s+(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})\s+(\d+)',
    re.IGNORECASE | re.DOTALL,
)


PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>生产图片预览</title>
  <style>
    :root{color-scheme:dark;--bg:#0b0d11;--panel:#151922;--line:#293141;--muted:#939db0;--accent:#7aa2ff}
    *{box-sizing:border-box}body{margin:0;min-height:100vh;background:radial-gradient(circle at 20% 0,#172036 0,transparent 35%),var(--bg);color:#f5f7fb;font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
    header{position:sticky;top:0;z-index:5;display:flex;gap:14px;align-items:center;padding:14px 22px;background:#0b0d11e8;border-bottom:1px solid var(--line);backdrop-filter:blur(16px)}
    h1{margin:0;font-size:18px;white-space:nowrap}.count,.source{color:var(--muted);white-space:nowrap}.source{max-width:230px;overflow:hidden;text-overflow:ellipsis}
    .controls{display:flex;gap:10px;width:100%;justify-content:flex-end}input,select,button{color:inherit;background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:8px 11px;outline:none}input{width:min(330px,40vw)}input:focus,select:focus{border-color:var(--accent)}button{cursor:pointer}
    main{padding:22px}#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:16px}.card{min-width:0;overflow:hidden;border:1px solid var(--line);border-radius:13px;background:var(--panel);cursor:zoom-in;transition:.16s ease}.card:hover{transform:translateY(-2px);border-color:#485875;box-shadow:0 10px 30px #0007}
    .preview{display:grid;place-items:center;aspect-ratio:1;background:linear-gradient(135deg,#11151d,#1c2230);overflow:hidden}.preview img{width:100%;height:100%;object-fit:contain}.info{padding:10px 11px 12px}.name{overflow:hidden;white-space:nowrap;text-overflow:ellipsis;font-weight:600}.meta{margin-top:3px;color:var(--muted);font-size:12px}.empty{grid-column:1/-1;display:grid;place-items:center;min-height:55vh;color:var(--muted);text-align:center}
    dialog{width:100vw;height:100vh;max-width:none;max-height:none;margin:0;padding:0;border:0;background:#050609ed;color:#fff}dialog::backdrop{background:#050609ed}.viewer{width:100%;height:100%;display:grid;grid-template-rows:1fr auto}.stage{position:relative;min-height:0;display:grid;place-items:center;padding:52px 70px 20px}.stage img{max-width:100%;max-height:100%;object-fit:contain}.close,.nav{position:absolute;z-index:2;border:0;background:#161a22cc;font-size:24px}.close{top:14px;right:18px}.nav{top:50%;transform:translateY(-50%);width:45px;height:58px}.prev{left:16px}.next{right:16px}.caption{padding:13px 22px 18px;text-align:center;color:#c7cfdd;word-break:break-all}
    @media(max-width:700px){header{flex-wrap:wrap}.source{display:none}.controls{justify-content:stretch}input{flex:1;width:auto}main{padding:12px}#grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.stage{padding:50px 10px 75px}.nav{top:auto;bottom:16px}}
  </style>
</head>
<body>
  <header><h1>生产图片</h1><span class="count" id="count">加载中…</span><span class="source" id="source"></span><div class="controls"><input id="search" type="search" placeholder="搜索文件名" autocomplete="off"><select id="sort"><option value="modified">最新优先</option><option value="name">按名称</option><option value="size">按大小</option></select><button id="refresh">刷新</button></div></header>
  <main><section id="grid"></section></main>
  <dialog id="lightbox"><div class="viewer"><div class="stage"><button class="close" aria-label="关闭">×</button><button class="nav prev" aria-label="上一张">‹</button><img id="large" alt=""><button class="nav next" aria-label="下一张">›</button></div><div class="caption" id="caption"></div></div></dialog>
  <script>
    const grid=document.querySelector('#grid'),count=document.querySelector('#count'),search=document.querySelector('#search'),sort=document.querySelector('#sort'),box=document.querySelector('#lightbox'),large=document.querySelector('#large'),caption=document.querySelector('#caption');let images=[],visible=[],current=0;
    const size=n=>n<1024?`${n} B`:n<1048576?`${(n/1024).toFixed(1)} KB`:`${(n/1048576).toFixed(1)} MB`;
    function render(){const q=search.value.trim().toLocaleLowerCase();visible=images.filter(x=>x.name.toLocaleLowerCase().includes(q));visible.sort(sort.value==='name'?(a,b)=>a.name.localeCompare(b.name,'zh-CN',{numeric:true}):sort.value==='size'?(a,b)=>b.size-a.size:(a,b)=>b.modified-a.modified);count.textContent=`${visible.length} / ${images.length} 张`;if(!visible.length){grid.innerHTML=`<div class="empty">${images.length?'没有匹配的图片':'生产目录里没有图片'}</div>`;return}grid.replaceChildren(...visible.map((x,i)=>{const card=document.createElement('article');card.className='card';card.title=x.name;const preview=document.createElement('div');preview.className='preview';const img=document.createElement('img');img.src=x.thumbnailUrl;img.alt=x.name;img.loading='lazy';img.decoding='async';const info=document.createElement('div');info.className='info';const name=document.createElement('div');name.className='name';name.textContent=x.name;const meta=document.createElement('div');meta.className='meta';meta.textContent=`${size(x.size)} · ${new Date(x.modified*1000).toLocaleString()}`;preview.append(img);info.append(name,meta);card.append(preview,info);card.onclick=()=>openAt(i);return card}))}
    function openAt(i){if(!visible.length)return;current=(i+visible.length)%visible.length;const x=visible[current];large.src=x.url;large.alt=x.name;caption.textContent=`${x.name} · ${size(x.size)}`;if(!box.open)box.showModal()}
    async function load(){count.textContent='读取生产目录…';try{const r=await fetch('/api/images',{cache:'no-store'});if(!r.ok)throw new Error(await r.text());const data=await r.json();images=data.images;document.querySelector('#source').textContent=data.source;document.querySelector('#source').title=data.source;render()}catch(e){grid.innerHTML=`<div class="empty">读取失败：${String(e)}</div>`;count.textContent='读取失败'}}
    search.oninput=render;sort.onchange=render;document.querySelector('#refresh').onclick=load;document.querySelector('.close').onclick=()=>box.close();document.querySelector('.prev').onclick=()=>openAt(current-1);document.querySelector('.next').onclick=()=>openAt(current+1);box.onclick=e=>{if(e.target===box)box.close()};addEventListener('keydown',e=>{if(!box.open)return;if(e.key==='ArrowLeft')openAt(current-1);if(e.key==='ArrowRight')openAt(current+1)});load();
  </script>
</body>
</html>"""


def read_remote_images(source: str) -> list[dict[str, object]]:
    request = Request(source, headers={"User-Agent": "csbot-production-image-gallery/1.0"})
    with urlopen(request, timeout=10) as response:  # noqa: S310 - URL is supplied by the operator
        charset = response.headers.get_content_charset() or "utf-8"
        listing = response.read(2 * 1024 * 1024).decode(charset, errors="replace")

    images: list[dict[str, object]] = []
    for raw_href, raw_modified, raw_size in ENTRY_PATTERN.findall(listing):
        href = html.unescape(raw_href)
        name = unquote(PurePosixPath(urlsplit(href).path).name)
        if PurePosixPath(name).suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue
        modified = datetime.strptime(raw_modified, "%d-%b-%Y %H:%M").timestamp()
        origin = f"{urlsplit(source).scheme}://{urlsplit(source).netloc}"
        images.append(
            {
                "name": name,
                "url": urljoin(source, href),
                "thumbnailUrl": f"{origin}/api/images/pic/thumbnail/{quote(name, safe='')}?size=320",
                "size": int(raw_size),
                "modified": modified,
            }
        )
    return images


class GalleryHandler(BaseHTTPRequestHandler):
    source: str

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path == "/":
            self._send(PAGE.encode(), "text/html; charset=utf-8")
            return
        if path == "/api/images":
            try:
                payload = {"source": self.source, "images": read_remote_images(self.source)}
            except Exception as exc:
                self.send_error(HTTPStatus.BAD_GATEWAY, f"无法读取生产图片目录：{exc}")
                return
            self._send(json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def _send(self, data: bytes, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, message: str, *args: object) -> None:
        print(f"[{datetime.now():%H:%M:%S}] {message % args}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="不落盘预览生产服务器图片")
    parser.add_argument("--source", default="http://42.193.244.178:1234/imgs/pic/", help="生产图片目录索引 URL")
    parser.add_argument("--host", default="127.0.0.1", help="本地监听地址")
    parser.add_argument("--port", type=int, default=8765, help="本地监听端口")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = args.source.rstrip("/") + "/"
    handler = type("ConfiguredGalleryHandler", (GalleryHandler,), {"source": source})
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"生产目录：{source}")
    print(f"本地预览：http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
