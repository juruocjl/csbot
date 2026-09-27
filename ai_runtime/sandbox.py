"""Run untrusted Python in a disposable Linux container; fail closed if absent."""
import asyncio
import base64
import json
import os
from pathlib import Path
import re
import tempfile
import uuid


class ScriptSandbox:
    def __init__(self, dispatch, artifacts: Path, image="csbot-ai-python:1"):
        self.dispatch,self.artifacts,self.image = dispatch,artifacts,image

    async def run(self, code: str):
        if not isinstance(code,str) or len(code.encode()) > 64*1024:
            raise ValueError("script must be <=64 KiB")
        name = "csbot-ai-"+uuid.uuid4().hex
        count = 0
        tasks = set()
        async def serve(reader,writer):
            nonlocal count
            tasks.add(asyncio.current_task())
            try:
                count += 1
                if count > 32:
                    raise ValueError("query budget exhausted (32 calls per script)")
                raw = await asyncio.wait_for(reader.readline(),2)
                if len(raw)>32*1024:
                    raise ValueError("request too large")
                request = json.loads(raw)
                result = await asyncio.wait_for(self.dispatch(request),15)
                reply = {"result":result}
            except Exception as exc:
                # Never return driver exceptions with DSNs, SQL or host paths.
                reply = {"error":str(exc)[:500] if isinstance(exc,ValueError) else type(exc).__name__}
            try:
                encoded=json.dumps(reply,ensure_ascii=False).encode()+b"\n"
                if len(encoded)>512*1024:
                    encoded=b'{"error":"result too large"}\n'
                writer.write(encoded)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                tasks.discard(asyncio.current_task())

        with tempfile.TemporaryDirectory(prefix="csbot-broker-") as directory:
            # Docker mounts only this empty transport directory, never secrets,
            # source checkout, model credentials, DB files or the Docker socket.
            os.chmod(directory,0o755)
            socket_path = Path(directory)/"data.sock"
            server = await asyncio.start_unix_server(serve,str(socket_path),limit=32769)
            os.chmod(socket_path,0o666)
            command = ["docker","run","--rm","-i","--name",name,"--network","none",
                       "--read-only","--cap-drop","ALL","--security-opt","no-new-privileges",
                       "--pids-limit","32","--memory","192m","--memory-swap","192m","--cpus","0.5",
                       "--ulimit","fsize=8388608:8388608","--ulimit","nofile=64:64",
                       "--tmpfs","/work:rw,noexec,nosuid,nodev,size=32m,mode=1777",
                       "--tmpfs","/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777",
                       "--mount",f"type=bind,src={directory},dst=/broker,readonly",
                       self.image]
            process = None
            output=bytearray()
            async def capture():
                while chunk := await process.stdout.read(16384):
                    output.extend(chunk)
                    if len(output)>4*1024**2:
                        raise ValueError("script output exceeded 4 MiB")
            try:
                process = await asyncio.create_subprocess_exec(*command,stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
                async with asyncio.timeout(45):
                    process.stdin.write(code.encode())
                    await process.stdin.drain()
                    process.stdin.close()
                    await capture()
                    await process.wait()
                if process.returncode == 125:
                    raise ValueError("isolated script runner unavailable; deployment preflight required")
            finally:
                # Docker CLI termination alone does not terminate the container.
                try:
                    cleanup=await asyncio.create_subprocess_exec("docker","rm","-f",name,
                        stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
                    await asyncio.wait_for(cleanup.wait(),10)
                finally:
                    if process:
                        # A full pipe pauses asyncio's transport and can keep
                        # wait() pending even after SIGKILL. Drain and discard
                        # remaining output while reaping the terminated CLI.
                        async def discard():
                            while await process.stdout.read(16384): pass
                        remaining=asyncio.create_task(discard())
                        if process.returncode is None: process.kill()
                        await process.wait()
                        await remaining
                    server.close()
                    await server.wait_closed()
                    for task in list(tasks): task.cancel()
                    await asyncio.gather(*list(tasks),return_exceptions=True)
            files,lines=[],[]
            self.artifacts.mkdir(parents=True,exist_ok=True)
            for line in output.decode(errors="replace").splitlines():
                if line.startswith("CSBOT_ARTIFACT:"):
                    value=json.loads(line.removeprefix("CSBOT_ARTIFACT:"))
                    filename=value["name"]
                    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}\.(png|jpg|jpeg|txt|csv|json)",filename):
                        raise ValueError("invalid artifact name")
                    data=base64.b64decode(value["data"],validate=True)
                    if len(data)>2*1024**2 or len(files)>=8:
                        raise ValueError("artifact budget exceeded")
                    path=self.artifacts/(uuid.uuid4().hex[:8]+"-"+filename)
                    path.write_bytes(data)
                    files.append(str(path.resolve()))
                else:
                    lines.append(line)
            text="\n".join(lines)
            return {"exit_code":process.returncode,"output":text[:24000],"truncated":len(text)>24000,"artifacts":files}
