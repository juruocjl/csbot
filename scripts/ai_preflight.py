"""Readiness gate. Loads secrets locally but never prints configuration values."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def cleanup():
    ids=subprocess.check_output(['docker','ps','-aq','--filter','label=csbot.ai.sandbox=true'],text=True,timeout=10).split()
    if ids: subprocess.run(['docker','rm','-f',*ids],check=True,timeout=20,stdout=subprocess.DEVNULL)
    print(f'Cleaned {len(ids)} abandoned CSBot script containers')


def _daemon_token(values):
    """Read the control-plane token locally; it is never printed or logged."""
    token = str(values.get('CS_AI_DSH_DAEMON_TOKEN') or '').strip()
    if token:
        return token
    path = str(values.get('CS_AI_DSH_DAEMON_TOKEN_FILE') or '').strip()
    if not path:
        return ''
    try:
        return Path(path).read_text(encoding='utf-8').strip()
    except OSError:
        return ''


def check_dsh_daemon(values):
    """Loopback-only /health probe of the resident DSH daemon (stdlib only)."""
    import urllib.error
    import urllib.parse
    import urllib.request
    url = str(values.get('CS_AI_DSH_DAEMON_URL') or '').strip().rstrip('/')
    parsed = urllib.parse.urlsplit(url)
    try:
        port = parsed.port
    except ValueError:
        port = None
    if (parsed.scheme != 'http'
            or (parsed.hostname or '') not in {'127.0.0.1', '::1', 'localhost'}
            or not port):
        raise ValueError('CS_AI_DSH_DAEMON_URL must be a loopback HTTP endpoint with a port')
    request = urllib.request.Request(url + '/health', method='GET',
                                     headers={'Accept': 'application/json'})
    token = _daemon_token(values)
    if token:
        request.add_header('Authorization', 'Bearer ' + token)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=5) as response:
            payload = json.loads(response.read(1024 * 1024))
    except urllib.error.HTTPError as exc:
        exc.close()
        raise ValueError('DSH daemon health endpoint rejected the probe')
    if not isinstance(payload, dict) or payload.get('ok') is not True:
        raise ValueError('DSH daemon health check is not ok')


async def check(args):
    from dotenv import dotenv_values
    values={}
    for path in args.env:
        if not path.is_file(): raise ValueError('environment file missing')
        values.update({key.upper():value for key,value in dotenv_values(path).items() if value is not None})
    values.update(os.environ)
    if sys.version_info<(3,11): raise ValueError('Python >=3.11 required')
    node=shutil.which('node')
    if not node: raise ValueError('Node executable missing from service PATH')
    version=subprocess.check_output([node,'-p','process.versions.node'],text=True,timeout=5).strip()
    if tuple(map(int,version.split('.')[:2]))<(22,12): raise ValueError('Node >=22.12 required')
    subprocess.run([node,'--input-type=module','-e',"import 'node:sqlite'"],check=True,capture_output=True,timeout=5)
    runtime=ROOT/'ai_runtime/dsh'
    for name,want in json.loads((runtime/'package.json').read_text())['dependencies'].items():
        installed=runtime/'node_modules'/name/'package.json'
        if not installed.is_file() or json.loads(installed.read_text())['version']!=want:
            raise ValueError('DSH/Mneme dependency version mismatch; run npm ci --ignore-scripts')
    print(f'PASS: Python, Node {version}, pinned DSH/Mneme')
    from ai_runtime.source import reviewed_files
    reviewed_files()
    print('PASS: reviewed business source manifest matches deployed files')
    if str(values.get('CS_AI_DSH_MODE', 'subprocess')).strip().lower() == 'daemon':
        check_dsh_daemon(values)
        print('PASS: resident DSH daemon health endpoint is ok')
    subprocess.run(['docker','image','inspect','csbot-ai-python:1','--format','{{.Id}}'],check=True,capture_output=True,timeout=10)
    from ai_runtime.sandbox import ScriptSandbox
    async def deny(_): raise ValueError('preflight has no script data access')
    with tempfile.TemporaryDirectory(prefix='csbot-preflight-') as tmp:
        result=await ScriptSandbox(deny,Path(tmp)).run('import numpy, matplotlib, PIL; print("ready")')
        if result['exit_code'] or 'ready' not in result['output']: raise ValueError('sandbox execution failed')
    print('PASS: service identity can run the restricted script image')
    from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
    from sqlalchemy import text
    from ai_runtime.data import DataBroker
    from ai_runtime.queries import TABLES,compile_query
    database=values.get('CS_DATABASE')
    if not database or not values.get('CS_AI_API_KEY') or not values.get('CS_AI_URL'):
        raise ValueError('required database/model configuration missing')
    engine=create_async_engine(database)
    try:
        broker=DataBroker(async_sessionmaker(engine),'0',Path(values['CS_AI_GAME_DB']) if values.get('CS_AI_GAME_DB') else None)
        for name in TABLES:
            await broker.query(f'SELECT * FROM {name} LIMIT 0')
        async with engine.connect() as connection:
            await connection.execute(text('SELECT token_text,token_count FROM chat_retrieval_span WHERE false'))
            await connection.execute(text('SELECT 1 FROM chat_token_lexicon WHERE false'))
            await connection.execute(text('SELECT mem FROM ai_mem WHERE false'))
        broker._game(compile_query('SELECT h.*,n.* FROM game_history h LEFT JOIN game_names n ON h.game_id=n.game_id LIMIT 0',{},'0','game'))
        print('PASS: main/game schemas and readonly access; no business rows returned')
        health=await asyncio.to_thread(broker.monitor_health)
        if not health['ok']:
            if not args.allow_monitor_unready: raise ValueError('Steam Monitor is not ready')
            print('WARNING: Monitor not ready; AI must report historical data as historical')
    finally:
        await engine.dispose()
    state=Path(values.get('CS_AI_STATE_DIR','data/ai'))
    state.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryFile(dir=state): pass
    if shutil.disk_usage(state).free<800*1024**2: raise ValueError('less than 800 MiB free disk space')
    print('PASS: Monitor readiness, state write access, free disk space')
    print('READY: model communication and protected API still require release acceptance')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env',type=Path,action='append',default=[])
    parser.add_argument('--allow-monitor-unready',action='store_true')
    parser.add_argument('--cleanup-sandboxes',action='store_true',help='Only with backend stopped; removes containers carrying our exact label')
    args=parser.parse_args()
    try:
        if args.cleanup_sandboxes: cleanup()
        else: asyncio.run(check(args))
    except Exception as exc:
        # Driver exceptions may contain DSNs/SQL. Deliberately print only type,
        # except our own fixed ValueErrors which contain no credential values.
        print('NOT READY:',str(exc) if type(exc) is ValueError else type(exc).__name__)
        raise SystemExit(1)


if __name__=='__main__':main()
