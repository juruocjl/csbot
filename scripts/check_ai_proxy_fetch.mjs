/** Loopback-only transport checks; public destinations are simulated by the proxy. */
import assert from 'node:assert/strict';
import http from 'node:http';
import {once} from 'node:events';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFileSync} from 'node:child_process';
import test from 'node:test';
import {ProxyFetchProvider, validateProxyUrl} from '../ai_runtime/dsh/proxy-fetch.mjs';
import {getGlobalDispatcher} from '../ai_runtime/dsh/node_modules/undici/index.js';

test('proxy URL is fixed by the operator and accepts only unauthenticated loopback HTTP', () => {
  assert.equal(validateProxyUrl('http://127.0.0.1:7891'), 'http://127.0.0.1:7891');
  assert.equal(validateProxyUrl('http://[::1]:7891'), 'http://[::1]:7891');
  for (const url of ['https://127.0.0.1:7891', 'http://localhost:7891', 'http://example.com',
    'http://user:secret@127.0.0.1', 'http://127.0.0.1/path', 'http://127.0.0.1/?key=x',
    'http://127.0.0.1/#fragment', 'socks5://127.0.0.1:7891']) assert.throws(() => validateProxyUrl(url));
});

test('proxy transport preserves native public URL, redirect, size and cancellation controls', async t => {
  const tunnels = [], requests = [], sockets = new Set();
  const proxy = http.createServer();
  proxy.on('connection', socket => {sockets.add(socket); socket.on('close', () => sockets.delete(socket));});
  proxy.on('connect', (req, socket) => {
    tunnels.push(req.url);
    socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
    socket.once('data', chunk => {
      const request = chunk.toString(); requests.push(request);
      const path = request.split(' ')[1];
      if (path === '/hang') return;
      let status = '200 OK', headers = 'Content-Type: text/plain\r\n', body = 'fixture';
      if (path === '/redirect') {status = '302 Found'; headers += 'Location: /ok\r\n'; body = '';}
      if (path === '/cross') {status = '302 Found'; headers += 'Location: http://example.net/\r\n'; body = '';}
      if (path === '/private-redirect') {status = '302 Found'; headers += 'Location: http://127.0.0.1/\r\n'; body = '';}
      if (path === '/loop') {status = '302 Found'; headers += 'Location: /loop\r\n'; body = '';}
      if (path === '/binary') headers = 'Content-Type: image/png\r\n';
      if (path === '/oversize') headers += 'Content-Length: 1000000\r\n';
      if (path === '/denied') status = '403 Forbidden';
      socket.end(`HTTP/1.1 ${status}\r\n${headers}Connection: close\r\n\r\n${body}`);
    });
  });
  proxy.listen(0, '127.0.0.1'); await once(proxy, 'listening');
  const uri = `http://127.0.0.1:${proxy.address().port}`;
  const provider = new ProxyFetchProvider(uri);
  const get = (path, instance = provider, signal) => instance.fetch({url: 'http://93.184.216.34'+path}, signal);
  try {
    await t.test('CONNECT uses the validated IP while HTTP Host keeps the original domain', async () => {
      const global = getGlobalDispatcher();
      const pinned = new ProxyFetchProvider(uri, {}, async () => [{address: '93.184.216.34', family: 4}]);
      const result = await pinned.fetch({url: 'http://example.com/ok'});
      assert.equal(result.body.content, 'fixture');
      assert.equal(tunnels.at(-1), '93.184.216.34:80');
      assert.match(requests.at(-1), /host: example\.com\r\n/i);
      assert.doesNotMatch(requests.at(-1), /authorization:|cookie:|x-api-key:/i);
      assert.equal(getGlobalDispatcher(), global);
    });
    await t.test('private literals and DNS names, credentials and non-HTTP schemes never reach the proxy', async () => {
      const count = tunnels.length;
      for (const url of ['http://127.0.0.1/', 'http://[::1]/', 'http://169.254.169.254/',
        'http://10.0.0.1/', 'http://[::ffff:127.0.0.1]/', 'http://localhost/',
        'https://user:secret@example.com/', 'file:///etc/passwd'])
        await assert.rejects(provider.fetch({url}));
      assert.equal(tunnels.length, count);
    });
    await t.test('same-origin redirect works; private/cross-origin and excessive redirects fail', async () => {
      assert.equal((await get('/redirect')).url, 'http://93.184.216.34/ok');
      for (const path of ['/cross', '/private-redirect', '/loop'])
        await assert.rejects(get(path), {code: 'WEB_REDIRECT_BLOCKED'});
    });
    await t.test('non-2xx status stays descriptive; binary/oversized bodies fail and streamed bodies are capped', async () => {
      assert.equal((await get('/denied')).statusCode, 403);
      await assert.rejects(get('/binary'), {code: 'WEB_UNSUPPORTED_CONTENT_TYPE'});
      await assert.rejects(get('/oversize'), {code: 'WEB_FETCH_TOO_LARGE'});
      const result = await get('/ok', new ProxyFetchProvider(uri, {maxResponseBytes: 4}));
      assert.equal(result.body.content, 'fixt'); assert.equal(result.truncated, true);
    });
    await t.test('timeout and caller cancellation terminate stalled work', async () => {
      await assert.rejects(get('/hang', new ProxyFetchProvider(uri, {timeoutMs: 50})), {code: 'WEB_FETCH_TIMEOUT'});
      await assert.rejects(get('/hang', provider, AbortSignal.timeout(50)), {code: 'WEB_ABORTED'});
    });
    await t.test('a failed proxy never falls back to a direct request', async () => {
      const closed = http.createServer(); closed.listen(0, '127.0.0.1'); await once(closed, 'listening');
      const url = `http://127.0.0.1:${closed.address().port}`; await new Promise(resolve => closed.close(resolve));
      await assert.rejects(get('/ok', new ProxyFetchProvider(url)), {code: 'WEB_PROVIDER_ERROR'});
    });
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => proxy.close(resolve));
  }
});

test('proxied HTTPS preserves SNI and certificate hostname verification', () => {
  const directory = mkdtempSync(join(tmpdir(), 'csbot-proxy-tls-'));
  const key = join(directory, 'fixture.key'), cert = join(directory, 'fixture.pem');
  try {
    execFileSync('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', key,
      '-out', cert, '-subj', '/CN=example.com', '-addext', 'subjectAltName=DNS:example.com', '-days', '1'], {stdio: 'ignore'});
    // A separate test process trusts only this newly generated fixture CA in
    // addition to the normal roots; production TLS validation stays enabled.
    execFileSync(process.execPath, ['--input-type=module', '-e', `
      import assert from 'node:assert/strict';
      import https from 'node:https'; import http from 'node:http'; import net from 'node:net';
      import {readFileSync} from 'node:fs'; import {once} from 'node:events';
      import {ProxyFetchProvider} from ${JSON.stringify(new URL('../ai_runtime/dsh/proxy-fetch.mjs', import.meta.url).href)};
      const tunnels=[], sockets=new Set(), sni=[];
      const origin=https.createServer({key:readFileSync(process.argv[1]),cert:readFileSync(process.argv[2])},(req,res)=>{
        assert.equal(req.headers.host,'example.com');res.setHeader('Content-Type','text/plain');res.end('TLS fixture');
      });
      origin.on('tlsClientError',()=>{});origin.on('secureConnection',s=>sni.push(s.servername));
      origin.listen(0,'127.0.0.1');await once(origin,'listening');
      const proxy=http.createServer();
      proxy.on('connection',s=>{sockets.add(s);s.on('close',()=>sockets.delete(s));});
      proxy.on('connect',(req,s)=>{
        tunnels.push(req.url);const upstream=net.connect(origin.address().port,'127.0.0.1');
        sockets.add(upstream);upstream.on('close',()=>sockets.delete(upstream));upstream.on('error',()=>s.destroy());
        upstream.once('connect',()=>{s.write('HTTP/1.1 200 Connection Established\\r\\n\\r\\n');s.pipe(upstream);upstream.pipe(s);});
        s.on('close',()=>upstream.destroy());
      });
      proxy.listen(0,'127.0.0.1');await once(proxy,'listening');
      try{
        const provider=new ProxyFetchProvider('http://127.0.0.1:'+proxy.address().port,{},async()=>[{address:'93.184.216.34',family:4}]);
        assert.equal((await provider.fetch({url:'https://example.com/'})).body.content,'TLS fixture');
        assert.equal(tunnels[0],'93.184.216.34:443');assert.equal(sni[0],'example.com');
        await assert.rejects(provider.fetch({url:'https://wrong.example.com/'}),{code:'WEB_PROVIDER_ERROR'});
      }finally{
        for(const s of sockets)s.destroy();await new Promise(r=>proxy.close(r));await new Promise(r=>origin.close(r));
      }
    `, key, cert], {env: {...process.env, NODE_EXTRA_CA_CERTS: cert}, stdio: 'pipe', timeout: 15000});
  } finally { rmSync(directory, {recursive: true, force: true}); }
});
