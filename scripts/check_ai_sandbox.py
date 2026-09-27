"""Linux/Docker integration check, using synthetic data and no credentials."""
import asyncio
from pathlib import Path
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ai_runtime.sandbox import ScriptSandbox
from ai_runtime.source import source_snapshot


async def main():
    async def fixture(request):
        if request.get("method")=="call" and request.get("name")=="members":
            return {"rows":[{"uid":"fixture","steamid":"fixture"}],"truncated":False}
        raise ValueError("only synthetic members fixture is available")
    with tempfile.TemporaryDirectory(prefix="csbot-sandbox-check-") as tmp, source_snapshot() as source:
        sandbox=ScriptSandbox(fixture,Path(tmp)/"artifacts",source_root=source.root)
        code='''import csdata,json,os,socket
from pathlib import Path
assert Path("/source/plugins/fudu/__init__.py").is_file()
assert "calc_roll_point" in Path("/source/plugins/fudu/__init__.index.md").read_text()
assert not Path("/source/.env.prod").exists()
assert not Path("/source/.git").exists()
assert not Path("/source/data").exists()
for target in (Path("/source/plugins/fudu/__init__.py"), Path("/source/new-file.py")):
    try: target.write_text("attempted mutation")
    except OSError: pass
    else: raise AssertionError("source snapshot was writable")
assert csdata.call("members")["rows"][0]["uid"]=="fixture"
assert not Path("/var/run/docker.sock").exists()
assert not any(key in os.environ for key in ("CS_DATABASE","DEEPSEEK_API_KEY","CS_TEST_TOKEN"))
try:
    Path("/etc/escape-test").write_text("bad")
except OSError:
    pass
else:
    raise AssertionError("root filesystem is writable")
sock=socket.socket();sock.settimeout(1)
try:
    sock.connect(("1.1.1.1",443))
except OSError:
    pass
else:
    raise AssertionError("external network reachable")
assert "0000000000000000" in next(line for line in Path("/proc/self/status").read_text().splitlines() if line.startswith("CapEff:"))
assert Path("/sys/fs/cgroup/memory.max").read_text().strip()==str(192*1024*1024)
assert Path("/sys/fs/cgroup/pids.max").read_text().strip()=="32"
Path("result.json").write_text(json.dumps({"fixture":True}))
csdata.artifact("result.json")
print("isolation fixture passed")
'''
        result=await sandbox.run(code)
        assert result["exit_code"]==0,result["output"]
        assert len(result["artifacts"])==1
        assert Path(result["artifacts"][0]).read_text()=='{"fixture": true}'
        print("PASS: read-only reviewed source mount and function search; scoped broker, no inherited credentials, no Docker socket, read-only root, no network, cgroup memory/PID limits, artifact export")
        result=await sandbox.run("""import csdata
csdata.configure_plot()
import matplotlib.pyplot as plt
plt.plot([1,2,3],[2,4,3]);plt.title('合成测试趋势');plt.savefig('chart.png');plt.close()
csdata.artifact('chart.png',send=True,caption='测试图')
""")
        assert result['exit_code']==0,result['output']
        assert len(result['deliver_images'])==1 and result['deliver_images'][0]['caption']=='测试图'
        assert Path(result['deliver_images'][0]['path']).read_bytes().startswith(b'\x89PNG')
        assert 'Glyph' not in result['output'],result['output']
        print('PASS: isolated matplotlib with Chinese font produces an explicitly submitted PNG')
        try:
            await sandbox.run("print('x' * (5 * 1024 * 1024))")
        except ValueError as exc:
            assert "output" in str(exc)
        else:
            raise AssertionError("output cap was not enforced")
        print("PASS: excess output rejected; disposable container cleaned up")


if __name__=="__main__":asyncio.run(main())
