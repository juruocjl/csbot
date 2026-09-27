"""Agent script SDK. The only transport is an invocation-owned Unix socket."""
import base64
import json
from pathlib import Path
import socket


def _request(payload):
    raw = json.dumps(payload,ensure_ascii=False).encode()+b"\n"
    if len(raw)>32*1024:
        raise ValueError("request too large")
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.settimeout(15)
        connection.connect("/broker/data.sock")
        connection.sendall(raw)
        stream = connection.makefile("rb")
        reply = stream.readline(512*1024)
    result = json.loads(reply)
    if "error" in result:
        raise RuntimeError(result["error"])
    return result["result"]


def catalog():
    return _request({"method":"catalog"})


def call(name, **params):
    return _request({"method":"call","name":name,"params":params})


def query(sql, params=None, source="main"):
    return _request({"method":"query","sql":sql,"params":params or {},"source":source})


def search(query, **filters):
    return _request({"method":"search","query":query,"filters":filters})


def block(block_id):
    return _request({"method":"block","id":block_id})


def image(image_id):
    """Return existing read-only paths/status; never silently re-download."""
    return _request({"method":"image","id":image_id})


def artifact(path):
    """Export a bounded PNG/JPEG/text/CSV/JSON result for this invocation."""
    file = Path(path)
    if file.is_symlink() or not file.is_file() or file.stat().st_size > 2*1024**2:
        raise ValueError("artifact must be a regular file <=2 MiB")
    if file.suffix.lower() not in {".png",".jpg",".jpeg",".txt",".csv",".json"}:
        raise ValueError("unsupported artifact type")
    print("CSBOT_ARTIFACT:"+json.dumps({"name":file.name,"data":base64.b64encode(file.read_bytes()).decode()}))
