// CSBot resident DSH Web host launcher.
//
// Boots the pinned `@deepseek-ai/dsh@0.1.5-rc.3` with `--profile web`, overlaid
// by a generated safety profile that disables every host capability CSBot does
// not need and mounts the `csbot-daemon-bridge` HTTP control plane. The WebUI
// stays bound to loopback and is read-only unless CSBOT_DSH_WEB_ALLOW_INTERACTIVE=1.
//
// Production loads `/home/ubuntu/csbot/.env.prod`, whose names are `CS_AI_*` (the
// same variables the Python backend reads). `resolveDaemonConfig` derives the
// DSH-side `CSBOT_*`/`OPENAI_*`/`DEEPSEEK_*` view from them under the same
// same-origin credential rules as `ai_runtime/openai_support.py`: a search or
// gateway key is never replayed against a different origin.
//
// This file is deployment scaffolding only: it never talks to a provider, the
// database, or a chat group by itself. The authenticated WebUI URL is written to
// a 0600 file instead of being printed with its token query string.
import {spawn} from 'node:child_process';
import {
  chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync,
} from 'node:fs';
import {randomBytes} from 'node:crypto';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const DSH_BIN = join(HERE, 'node_modules/@deepseek-ai/dsh/lib/bin.js');
const BRIDGE = join(HERE, 'daemon-bridge.mjs');
const MNEME = join(HERE, 'mneme.mjs');

export const DEFAULT_OPENAI_MODEL = 'gpt-6.2-sol';
export const DEFAULT_DEEPSEEK_MODEL = 'deepseek-flash';
export const DEFAULT_DEEPSEEK_BASE_URL = 'https://api.deepseek.com';
export const DEFAULT_SEARCH_URL = 'https://api.deepseek.com/anthropic/v1';
export const DEFAULT_OPENAI_PROXY = 'http://127.0.0.1:7890';
const PROXY_OFF = new Set(['', 'none', 'direct', 'off', 'false']);
const LOOPBACK_HOSTS = new Set(['127.0.0.1', 'localhost', '::1']);

const get = (env, key) => String(env[key] ?? '').trim();
const has = (env, key) => Object.prototype.hasOwnProperty.call(env, key);

function parseUrl(raw) {
  try {
    return new URL(String(raw || '').trim());
  } catch {
    return null;
  }
}
function hostnameOf(raw) {
  return (parseUrl(raw)?.hostname || '').toLowerCase();
}
function isOfficialDeepseekOrigin(raw) {
  const parsed = parseUrl(raw);
  if (!parsed) return false;
  return parsed.protocol === 'https:'
    && parsed.hostname.toLowerCase() === 'api.deepseek.com'
    && (parsed.port === '' || parsed.port === '443')
    && !parsed.username && !parsed.password
    && !parsed.search && !parsed.hash;
}
function isOfficialDeepseekSearchUrl(raw) {
  const value = String(raw || '').trim();
  return !value || isOfficialDeepseekOrigin(value);
}
function normalizeOpenAiBase(raw) {
  const value = String(raw || '').trim().replace(/\/+$/, '');
  if (!value) return '';
  const parsed = parseUrl(value);
  if (parsed && parsed.hostname.toLowerCase() === 'api.openai.com'
      && (parsed.pathname === '' || parsed.pathname === '/')) {
    return `${value}/v1`;
  }
  return value;
}
function normalizeDeepseekBase(raw) {
  const value = String(raw || '').trim().replace(/\/+$/, '') || DEFAULT_DEEPSEEK_BASE_URL;
  const parsed = parseUrl(value);
  if (parsed && parsed.hostname.toLowerCase() === 'api.deepseek.com'
      && ['', '/', '/v1'].includes(parsed.pathname)) {
    return `${parsed.protocol}//${parsed.host}`;
  }
  return value;
}
function validateLoopbackProxy(raw) {
  const value = String(raw || '').trim();
  if (!value) return '';
  const parsed = parseUrl(value);
  if (!parsed || parsed.protocol !== 'http:' || !LOOPBACK_HOSTS.has(parsed.hostname.toLowerCase())
      || parsed.username || parsed.password
      || (parsed.pathname !== '' && parsed.pathname !== '/') || parsed.search || parsed.hash) {
    throw new Error('LLM proxy must be a loopback HTTP endpoint without credentials or a path');
  }
  return value;
}

/**
 * Derive the daemon's provider/search/proxy configuration from a plain env map.
 *
 * Explicit `CSBOT_*` / `OPENAI_API_KEY` / `DEEPSEEK_API_KEY` values always win.
 * Otherwise the backend-side `CS_AI_*` names are adapted with the same
 * same-origin rules as `ai_runtime/openai_support.py`. Pure: no I/O, no process
 * exit, importable by the acceptance tests without starting a child process.
 */
export function resolveDaemonConfig(env = process.env) {
  const configuredProvider = get(env, 'CSBOT_PROVIDER').toLowerCase();
  const override = get(env, 'CS_AI_PROVIDER').toLowerCase();
  let derivedProvider;
  if (override === 'openai') derivedProvider = 'openai';
  else if (override === 'deepseek' || override === 'deepseek-official') derivedProvider = 'deepseek-official';
  else derivedProvider = hostnameOf(get(env, 'CS_AI_URL')) === 'api.openai.com' ? 'openai' : 'deepseek-official';
  const provider = configuredProvider === 'openai'
    ? 'openai'
    : configuredProvider ? 'deepseek-official' : derivedProvider;
  const isOpenAI = provider === 'openai';

  const aiUrl = get(env, 'CS_AI_URL');
  const aiKey = get(env, 'CS_AI_API_KEY');
  const searchKey = get(env, 'CSBOT_SEARCH_API_KEY') || get(env, 'CS_AI_SEARCH_API_KEY');
  const rawSearchUrl = get(env, 'CSBOT_SEARCH_URL') || get(env, 'CS_AI_SEARCH_URL');
  const officialSearch = isOfficialDeepseekSearchUrl(rawSearchUrl);

  let openaiApiKey = '';
  let openaiBaseUrl = '';
  let model = '';
  let deepseekApiKey = '';
  let deepseekBaseUrl = '';
  if (isOpenAI) {
    // `CS_AI_*` is only reused when it actually describes the OpenAI primary;
    // an explicit CSBOT_PROVIDER switch must not replay a DeepSeek credential.
    openaiApiKey = get(env, 'OPENAI_API_KEY')
      || (derivedProvider === 'openai' ? aiKey : '');
    openaiBaseUrl = normalizeOpenAiBase(
      get(env, 'CSBOT_OPENAI_BASE_URL')
      || (derivedProvider === 'openai' ? aiUrl : ''));
    model = get(env, 'CSBOT_MODEL') || get(env, 'CS_AI_MODEL') || DEFAULT_OPENAI_MODEL;
    deepseekApiKey = get(env, 'DEEPSEEK_API_KEY');
    deepseekBaseUrl = get(env, 'DEEPSEEK_BASE_URL');
    if (!deepseekApiKey && officialSearch && searchKey) {
      // Same-origin reuse only: the search credential belongs to api.deepseek.com.
      deepseekApiKey = searchKey;
      deepseekBaseUrl = DEFAULT_DEEPSEEK_BASE_URL;
    }
  } else {
    deepseekApiKey = get(env, 'DEEPSEEK_API_KEY')
      || (derivedProvider === 'deepseek-official' ? aiKey : '');
    deepseekBaseUrl = get(env, 'DEEPSEEK_BASE_URL')
      || (derivedProvider === 'deepseek-official' ? aiUrl : '')
      || DEFAULT_DEEPSEEK_BASE_URL;
    model = get(env, 'CSBOT_MODEL') || get(env, 'CS_AI_MODEL') || DEFAULT_DEEPSEEK_MODEL;
    // A native DeepSeek primary may still expose OpenAI for /admin/config.
    openaiApiKey = get(env, 'OPENAI_API_KEY');
    openaiBaseUrl = normalizeOpenAiBase(get(env, 'CSBOT_OPENAI_BASE_URL'));
  }
  deepseekBaseUrl = deepseekApiKey ? normalizeDeepseekBase(deepseekBaseUrl) : '';

  // Side channel (memory distill/verify) — never a cross-origin key replay.
  let aux = null;
  if (isOpenAI && get(env, 'CS_AI_AUX_ENABLE') !== '0') {
    const explicitKey = get(env, 'CS_AI_AUX_API_KEY');
    const explicitUrl = get(env, 'CS_AI_AUX_URL');
    const key = explicitKey || deepseekApiKey;
    const url = explicitUrl || deepseekBaseUrl || DEFAULT_DEEPSEEK_BASE_URL;
    const originOk = explicitKey ? true : isOfficialDeepseekOrigin(url);
    if (key && originOk) {
      aux = {
        provider: 'deepseek-official',
        model: get(env, 'CS_AI_AUX_MODEL') || DEFAULT_DEEPSEEK_MODEL,
        apiKey: key,
        baseUrl: normalizeDeepseekBase(url),
      };
    }
  }

  // Primary-channel fallback is orthogonal to the aux switch.
  let fallback = null;
  if (isOpenAI && get(env, 'CS_AI_OPENAI_FALLBACK_ENABLE') !== '0') {
    const explicitKey = get(env, 'CS_AI_FALLBACK_API_KEY');
    const explicitUrl = get(env, 'CS_AI_FALLBACK_URL');
    const key = explicitKey || aux?.apiKey || deepseekApiKey;
    const url = explicitUrl || aux?.baseUrl || deepseekBaseUrl || DEFAULT_DEEPSEEK_BASE_URL;
    const originOk = explicitKey ? true : isOfficialDeepseekOrigin(url);
    if (key && originOk) {
      fallback = {
        provider: 'deepseek-official',
        model: get(env, 'CS_AI_FALLBACK_MODEL') || aux?.model || DEFAULT_DEEPSEEK_MODEL,
        apiKey: key,
        baseUrl: normalizeDeepseekBase(url),
      };
    }
  }

  // Proxy: the GCP host dials providers directly. `CSBOT_DIRECT_EGRESS=1`
  // hard-disables any inherited proxy (a stale :7890 in .env.prod must not
  // resurface); otherwise an explicitly configured loopback proxy is honoured.
  let proxyRaw;
  if (get(env, 'CSBOT_DIRECT_EGRESS') === '1') proxyRaw = '';
  else if (has(env, 'CSBOT_LLM_PROXY_URL')) proxyRaw = get(env, 'CSBOT_LLM_PROXY_URL');
  else if (has(env, 'CS_AI_OPENAI_PROXY_URL')) proxyRaw = get(env, 'CS_AI_OPENAI_PROXY_URL');
  else proxyRaw = '';
  const llmProxyUrl = isOpenAI && !PROXY_OFF.has(proxyRaw.toLowerCase())
    ? validateLoopbackProxy(proxyRaw)
    : '';

  const webFlag = get(env, 'CSBOT_WEB_ENABLED');
  const webEnabled = webFlag === '1' || (webFlag !== '0' && Boolean(searchKey));
  // Web fetch proxy is a separate loopback knob in the backend (`CS_AI_FETCH_PROXY_URL`).
  const rawFetchProxy = get(env, 'CSBOT_DIRECT_EGRESS') === '1'
    ? ''
    : (get(env, 'CSBOT_FETCH_PROXY_URL') || get(env, 'CS_AI_FETCH_PROXY_URL'));
  let fetchProxyUrl = '';
  if (webEnabled && rawFetchProxy) fetchProxyUrl = validateLoopbackProxy(rawFetchProxy);
  return {
    provider,
    model,
    openaiApiKey,
    openaiBaseUrl,
    deepseekApiKey,
    deepseekBaseUrl,
    // `llm-pi-ai` is mounted whenever an OpenAI route exists, even when the
    // primary is native, so `/admin/config` can hot-switch models later.
    mountPiAi: isOpenAI || Boolean(openaiApiKey && openaiBaseUrl),
    llmProxyUrl,
    llmProxyHost: llmProxyUrl ? (hostnameOf(openaiBaseUrl) || 'api.openai.com') : '',
    auxProvider: aux?.provider || get(env, 'CSBOT_AUX_PROVIDER'),
    auxModel: aux?.model || get(env, 'CSBOT_AUX_MODEL'),
    auxApiKey: aux?.apiKey || '',
    auxBaseUrl: aux?.baseUrl || '',
    fallbackProvider: fallback?.provider || get(env, 'CSBOT_FALLBACK_PROVIDER'),
    fallbackModel: fallback?.model || get(env, 'CSBOT_FALLBACK_MODEL'),
    fallbackApiKey: fallback?.apiKey || '',
    fallbackBaseUrl: fallback?.baseUrl || '',
    webEnabled,
    browserFetchEnabled: webEnabled && get(env, 'CSBOT_BROWSER_FETCH_ENABLED') !== '0' && get(env, 'CS_AI_BROWSER_FETCH_ENABLE') !== '0',
    searchUrl: webEnabled ? (rawSearchUrl || DEFAULT_SEARCH_URL) : '',
    searchApiKey: webEnabled ? searchKey : '',
    searchModel: get(env, 'CSBOT_SEARCH_MODEL') || get(env, 'CS_AI_SEARCH_MODEL') || 'deepseek-v4-flash',
    fetchProxyUrl,
  };
}

function main() {
  const log = message => process.stdout.write(`csbot-dsh: ${message}\n`);
  const fail = message => {
    process.stderr.write(`csbot-dsh: ${message}\n`);
    process.exit(1);
  };

  // Any value that would escape the loopback control plane is rejected outright:
  // a malformed port or a protocol in a trusted host would silently widen the
  // browser-trust fence.
  const integer = (name, fallback, min, max) => {
    const raw = String(process.env[name] ?? '').trim();
    if (!raw) return fallback;
    if (!/^\d+$/.test(raw)) fail(`${name} must be an integer`);
    const value = Number(raw);
    if (value < min || value > max) fail(`${name} must be within ${min}..${max}`);
    return value;
  };
  const text = (name, fallback = '') => String(process.env[name] ?? fallback).trim();

  let config;
  try {
    config = resolveDaemonConfig(process.env);
  } catch (error) {
    fail(String(error?.message || error));
  }

  const DSH_HOME = text('DSH_HOME');
  if (!DSH_HOME) fail('DSH_HOME is required');
  const HOME = resolve(DSH_HOME);
  const SESSIONS = resolve(text('CSBOT_DSH_SESSIONS', join(HOME, 'sessions')));
  const MEMORY = resolve(text('CSBOT_DSH_MEMORY_DIR', join(HOME, 'memory')));
  const WEB_PORT = integer('CSBOT_DSH_WEB_PORT', 32150, 1, 65535);
  const BRIDGE_PORT = integer('CSBOT_DSH_BRIDGE_PORT', 32151, 1, 65535);
  if (WEB_PORT === BRIDGE_PORT) fail('CSBOT_DSH_WEB_PORT and CSBOT_DSH_BRIDGE_PORT must differ');
  const TOKEN_FILE = resolve(text('CSBOT_DSH_BRIDGE_TOKEN_FILE', join(HOME, 'bridge-token')));
  const LOGIN_URL_FILE = resolve(text('CSBOT_DSH_LOGIN_URL_FILE', join(HOME, 'login-url')));
  const PROFILE_FILE = join(HOME, 'daemon-profile.json');

  const TRUSTED_HOSTS = text('CSBOT_DSH_TRUSTED_HOSTS')
    .split(',')
    .map(value => value.trim())
    .filter(Boolean);
  for (const host of TRUSTED_HOSTS) {
    if (/[\s/]/.test(host) || host.includes('://') || host.includes(',')) {
      fail(`CSBOT_DSH_TRUSTED_HOSTS entry is not a bare host[:port]: ${JSON.stringify(host)}`);
    }
  }

  const ensurePrivateDir = path => {
    mkdirSync(path, {recursive: true, mode: 0o700});
    try { chmodSync(path, 0o700); } catch {}
  };
  const writePrivate = (path, value) => {
    writeFileSync(path, value, {mode: 0o600});
    try { chmodSync(path, 0o600); } catch {}
  };

  ensurePrivateDir(HOME);
  ensurePrivateDir(SESSIONS);
  ensurePrivateDir(MEMORY);

  if (!existsSync(DSH_BIN)) fail(`DSH runtime not installed at ${DSH_BIN}`);

  // The control-plane credential is generated once and only ever stored 0600.
  // The bridge plugin reads the same file, so rotation requires deleting it.
  if (!existsSync(TOKEN_FILE)) writePrivate(TOKEN_FILE, randomBytes(32).toString('hex') + '\n');
  const bridgeToken = readFileSync(TOKEN_FILE, 'utf8').trim();
  if (bridgeToken.length < 32) fail('bridge token file is invalid; remove it to regenerate');

  const {provider, model} = config;
  const contextWindow = integer('DSH_CONTEXT_WINDOW', provider === 'openai' ? 131072 : 65536, 4096, 2000000);
  const auxContextWindow = integer('CSBOT_AUX_CONTEXT_WINDOW', 65536, 4096, 2000000);
  const fallbackContextWindow = integer('CSBOT_FALLBACK_CONTEXT_WINDOW', 65536, 4096, 2000000);

  const modelRow = (id, window, modalities) => ({
    id, contextWindow: window, maxTokens: 8192, inputModalities: modalities,
  });

  const deepseekModels = [];
  for (const [id, window] of [
    [model, contextWindow],
    [config.fallbackModel, fallbackContextWindow],
    [config.auxModel, auxContextWindow],
  ]) {
    if (!id || deepseekModels.some(row => row.id === id)) continue;
    deepseekModels.push(modelRow(id, window, ['text']));
  }

  // Every dangerous or unnecessary host row is named explicitly. Rows absent from
  // the shipped web profile are harmless no-ops in a patch layer; naming them here
  // keeps the safety intent reviewable even if the bundled profile changes.
  const DISABLED = [
    'hmr',
    'session-telemetry-otel',
    // Workspace titles come from the CSBot request (`QQ群 …`), so an extra LLM
    // call per session would only add cost and latency.
    'session-title-llm',
    'persistent-bash', 'persistent-pwsh',
    'terminal-bash', 'terminal-pwsh', 'pty',
    'session-log-deepseek', 'plugin-package-inventory-deepseek',
    'tool-bash', 'tool-pwsh', 'tool-bash-persistent', 'tool-pwsh-persistent',
    'tool-str-replace-editor', 'tool-jobs', 'tool-subagent', 'tool-subagent-control',
    'tool-subagent-fork', 'tool-subagent-list-agents', 'tool-schedule', 'tool-goal',
    'tool-ralph', 'tool-workflow', 'tool-ask-user', 'tool-todo', 'tool-skill',
    'tool-skill-search', 'plan-mode', 'skill-filesystem',
    'playwright-mcp', 'windows-mcp', 'qq-companion', 'companion-preset',
    'matrix-bot-preset',
  ];

  const overlay = [
    ...DISABLED.map(id => ({id, disabled: true})),
    {id: 'compaction-basic', disabled: false, config: {}},
    {id: 'token-meter', config: {}},
    {id: 'tool-fs', disabled: false, config: {readLimit: 200, readMaxBytes: 24000}},
    {id: 'session-persistence-jsonl', config: {root: SESSIONS}},
    {id: 'agent-default-model', config: {provider, model}},
    {id: 'sandbox-policy', config: {mode: 'workspace-write', workspaceRoot: HOME}},
    {id: 'approval', config: {policy: 'never'}},
    {id: 'permission', config: {defaultPreset: 'workspace-write'}},
    {
      id: 'system-prompt',
      config: {
        includeRuntimeContext: true,
        includeHarnessIdentity: false,
        personaPrefix: 'You are a helpful assistant. Answer from the available evidence.',
        personaSuffix: '',
      },
    },
    {
      id: 'llm-deepseek',
      disabled: false,
      config: {
        apiKeyEnv: 'DEEPSEEK_API_KEY',
        defaultContextWindow: contextWindow,
        streamIdleTimeoutMs: 60000,
        maxTokens: 8192,
        maxImagesPerRequest: 8,
        imageOffloadCountQuantum: 2,
        models: deepseekModels.length ? deepseekModels : [modelRow(model, contextWindow, ['text'])],
      },
    },
    {
      insert: [
        // `fs` comes from the web profile's sandboxed backend; mounting
        // `dsh-fs-local` as well would register the service twice.
        {
          id: 'mneme',
          name: MNEME,
          config: {
            memoryDir: MEMORY,
            strictScope: true,
            autoDream: false,
            autoInject: false,
            autoSummarize: true,
            lightMode: true,
            entityExtractionEnabled: false,
            externalApiEnabled: false,
            ...(config.auxApiKey && config.auxProvider && config.auxModel
              ? {
                  summarizeProvider: config.auxProvider,
                  summarizeModel: config.auxModel,
                  summarizeReasoningEffort: 'off',
                }
              : {}),
          },
        },
        {
          id: 'csbot-daemon-bridge',
          name: BRIDGE,
          config: {},
        },
      ],
    },
  ];

  if (config.mountPiAi) {
    const modelProfile = {
      id: model,
      name: model,
      contextWindow,
      maxTokens: 16384,
      input: ['text', 'image'],
      reasoningEfforts: {off: 'low', low: 'low', medium: 'medium', high: 'high', xhigh: 'xhigh', max: 'max'},
    };
    overlay.push({
      id: 'llm-pi-ai',
      config: {
        providers: {
          openai: {
            apiKeyEnv: 'OPENAI_API_KEY',
            api: 'openai-responses',
            baseURL: config.openaiBaseUrl,
            streamIdleTimeoutMs: 120000,
            reasoning: 'low',
            models: [modelProfile],
            ...(config.fallbackProvider === 'deepseek-official' && config.fallbackModel
              ? {
                  retryPolicy: {
                    mode: 'normal',
                    maxRetries: 1,
                    retryableCodes: ['EMPTY_RESPONSE', 'SERVER'],
                    backoff: {initialDelayMs: 500, maxDelayMs: 1000, jitterRatio: 0.1},
                  },
                }
              : {}),
          },
        },
      },
    });
  }

  if (config.webEnabled) {
    overlay.push(
      {id: 'tool-web', disabled: false, config: {searchMaxQueries: 2, searchMaxResults: 6, searchTimeoutMs: 45000, fetchTimeoutMs: 18000, fetchMaxOutputChars: 300000}},
      {
        id: 'web-search-deepseek',
        config: {
          apiKeyEnv: 'CSBOT_SEARCH_API_KEY',
          baseURL: config.searchUrl,
          model: config.searchModel,
          maxTokens: 2048,
          maxUses: 2,
        },
      },
      {id: 'web-fetch-http', config: {maxResponseBytes: 500000, maxBodyChars: 300000, timeoutMs: 15000, maxRedirects: 3}},
    );
  }

  writePrivate(PROFILE_FILE, JSON.stringify(overlay, null, 2) + '\n');

  // The child gets a deliberately small environment: no bot token, no database URL,
  // no unrelated provider credential, and no ambient HTTP(S)_PROXY (it would
  // otherwise proxy the loopback control plane or a direct DeepSeek call).
  const PASS_THROUGH = [
    'PATH', 'HOME', 'USER', 'LANG', 'LC_ALL', 'TZ',
    'NODE_OPTIONS', 'NODE_EXTRA_CA_CERTS', 'SSL_CERT_FILE', 'SSL_CERT_DIR',
    'CSBOT_OPENAI_CONNECT_TIMEOUT_MS', 'CSBOT_OPENAI_PROBE',
    'CSBOT_FETCH_PROXY_URL', 'CSBOT_BROWSER_FETCH_ENABLED',
    'CSBOT_AUX_CONTEXT_WINDOW', 'CSBOT_FALLBACK_CONTEXT_WINDOW',
    'CSBOT_DSH_MAX_CONCURRENCY', 'CSBOT_DSH_WEB_ALLOW_INTERACTIVE',
    'CSBOT_DSH_TRUSTED_HOSTS', 'CSBOT_DSH_BRIDGE_PORT', 'CSBOT_DSH_BRIDGE_TOKEN_FILE',
    'CSBOT_DSH_STATE_ROOT', 'CSBOT_DSH_LOGIN_URL_FILE',
  ];
  const childEnv = {};
  for (const key of PASS_THROUGH) {
    if (typeof process.env[key] === 'string' && process.env[key] !== '') childEnv[key] = process.env[key];
  }
  Object.assign(childEnv, {
    DSH_HOME: HOME,
    DSH_TELEMETRY_DISABLED: '1',
    CSBOT_DSH_DAEMON: '1',
    CSBOT_DSH_MEMORY_DIR: MEMORY,
    CSBOT_DSH_SESSIONS: SESSIONS,
    CSBOT_DSH_LOGIN_URL_FILE: LOGIN_URL_FILE,
    CSBOT_DSH_BRIDGE_TOKEN_FILE: TOKEN_FILE,
    CSBOT_DSH_BRIDGE_PORT: String(BRIDGE_PORT),
    CSBOT_DSH_WEB_PORT: String(WEB_PORT),
    CSBOT_PROVIDER: provider,
    CSBOT_MODEL: model,
    ...(config.deepseekApiKey
      ? {DEEPSEEK_API_KEY: config.deepseekApiKey, DEEPSEEK_BASE_URL: config.deepseekBaseUrl}
      : {}),
    ...(config.openaiApiKey
      ? {OPENAI_API_KEY: config.openaiApiKey, CSBOT_OPENAI_BASE_URL: config.openaiBaseUrl}
      : {}),
    ...(config.llmProxyUrl
      ? {CSBOT_LLM_PROXY_URL: config.llmProxyUrl, CSBOT_LLM_PROXY_HOST: config.llmProxyHost}
      : {}),
    ...(config.auxProvider && config.auxModel
      ? {CSBOT_AUX_PROVIDER: config.auxProvider, CSBOT_AUX_MODEL: config.auxModel}
      : {}),
    ...(config.fallbackProvider && config.fallbackModel
      ? {CSBOT_FALLBACK_PROVIDER: config.fallbackProvider, CSBOT_FALLBACK_MODEL: config.fallbackModel}
      : {}),
    ...(config.webEnabled
      ? {
          CSBOT_WEB_ENABLED: '1',
          CSBOT_SEARCH_URL: config.searchUrl,
          CSBOT_SEARCH_API_KEY: config.searchApiKey,
          CSBOT_SEARCH_MODEL: config.searchModel,
          ...(config.fetchProxyUrl ? {CSBOT_FETCH_PROXY_URL: config.fetchProxyUrl} : {}),
          CSBOT_BROWSER_FETCH_ENABLED: config.browserFetchEnabled ? '1' : '0',
        }
      : {}),
    ...(get(process.env, 'CSBOT_OPENAI_CONNECT_TIMEOUT_MS') || get(process.env, 'CS_AI_OPENAI_CONNECT_TIMEOUT_MS')
      ? {CSBOT_OPENAI_CONNECT_TIMEOUT_MS: get(process.env, 'CSBOT_OPENAI_CONNECT_TIMEOUT_MS')
          || get(process.env, 'CS_AI_OPENAI_CONNECT_TIMEOUT_MS')}
      : {}),
    ...(text('CSBOT_DSH_STATE_ROOT') ? {CSBOT_DSH_STATE_ROOT: resolve(text('CSBOT_DSH_STATE_ROOT'))} : {}),
  });

  const args = [
    DSH_BIN, '--profile', 'web', '--patch', PROFILE_FILE,
    '--no-open', '--port', String(WEB_PORT), '--host', '127.0.0.1',
  ];
  for (const host of TRUSTED_HOSTS) args.push('--trusted-host', host);

  log(`starting resident DSH Web host on 127.0.0.1:${WEB_PORT} (control plane 127.0.0.1:${BRIDGE_PORT})`);
  const child = spawn(process.execPath, args, {
    cwd: HOME,
    env: childEnv,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  // Anything that looks like the authenticated deep link is moved into the 0600
  // file before it can reach syslog; other lines are forwarded verbatim.
  const scrub = line => line.replace(/([?&]token=)[^\s&"']+/gi, '$1[REDACTED]');
  const handleLine = (prefix, chunk) => {
    for (const raw of chunk.toString('utf8').split(/\r?\n/)) {
      if (!raw) continue;
      const match = /dsh web:\s*(\S+)/.exec(raw);
      if (match) {
        writePrivate(LOGIN_URL_FILE, match[1] + '\n');
        log('authenticated WebUI URL written to the login-url file');
        continue;
      }
      (prefix === 'err' ? process.stderr : process.stdout).write(scrub(raw) + '\n');
    }
  };
  child.stdout.on('data', chunk => handleLine('out', chunk));
  child.stderr.on('data', chunk => handleLine('err', chunk));

  let stopping = false;
  const stop = signal => {
    if (stopping) {
      // A second signal escalates immediately: systemd's TimeoutStopSec is short.
      try { child.kill('SIGKILL'); } catch {}
      return;
    }
    stopping = true;
    log(`received ${signal}; forwarding SIGTERM to the DSH host`);
    try { child.kill('SIGTERM'); } catch {}
    // DSH owns a bounded shutdown; give it 8s, then force it.
    setTimeout(() => { try { child.kill('SIGKILL'); } catch {} }, 8000).unref();
  };
  process.on('SIGTERM', () => stop('SIGTERM'));
  process.on('SIGINT', () => stop('SIGINT'));

  child.on('error', error => fail(`failed to start DSH: ${error.message}`));
  child.on('exit', (code, signal) => {
    log(`DSH host exited (code=${code ?? 'null'}, signal=${signal ?? 'none'})`);
    process.exit(code ?? (stopping ? 0 : 1));
  });
}

// Only the executed entry point starts a child process; importing this module
// for `resolveDaemonConfig` must stay side-effect free.
const invokedDirectly = process.argv[1]
  && pathToFileURL(resolve(process.argv[1])).href === import.meta.url;
if (invokedDirectly) main();
