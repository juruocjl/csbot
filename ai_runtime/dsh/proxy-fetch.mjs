/** Optional model-selected fetch through the operator's loopback HTTP proxy. */
import { Client, ProxyAgent, fetch } from 'undici';
import { HttpFetchProvider, DEFAULT_USER_AGENT } from '@deepseek-ai/dsh-web-fetch-http';
import { WebError } from '@deepseek-ai/dsh-web';
import { timeoutOf } from '@deepseek-ai/dsh-timeout';

export function validateProxyUrl(value) {
  let url;
  try { url = new URL(value); } catch { throw new Error('Fetch proxy must be a loopback HTTP endpoint.'); }
  if (url.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(url.hostname) ||
      url.username || url.password || url.pathname !== '/' || url.search || url.hash)
    throw new Error('Fetch proxy must be a loopback HTTP endpoint without credentials or a path.');
  return url.origin;
}

export class ProxyFetchProvider extends HttpFetchProvider {
  constructor(proxyUrl, limits = {}, resolveAddresses) {
    super({maxResponseBytes: 500000, maxBodyChars: 300000, timeoutMs: 15000,
      maxRedirects: 3, userAgent: DEFAULT_USER_AGENT, ...limits}, resolveAddresses);
    this.proxyUrl = validateProxyUrl(proxyUrl);
  }

  async requestOnce(url, signal) {
    // Reuse the locked native provider's full public-IP/DNS64 validation. Each
    // redirect resolves again; CONNECT receives that IP, never a second DNS name.
    const addresses = await this.resolveAddresses(url.hostname, signal);
    const address = addresses[0].address;
    const authority = `${addresses[0].family === 6 ? `[${address}]` : address}:${url.port || (url.protocol === 'https:' ? 443 : 80)}`;
    const dispatcher = new ProxyAgent({
      uri: this.proxyUrl, proxyTunnel: true, connectTimeout: 15000,
      clientFactory(origin, options) {
        const client = new Client(origin, options);
        const connect = client.connect.bind(client);
        client.connect = (params, ...args) => connect({...params, path: authority,
          headers: {...params.headers, host: authority}}, ...args);
        return client;
      },
    });
    try {
      const response = await fetch(url, {method: 'GET', redirect: 'manual',
        headers: {'user-agent': this.limits.userAgent,
          accept: 'text/html,application/xhtml+xml,text/*;q=0.9,application/json;q=0.8'},
        signal, dispatcher});
      // Keep the URL hostname for HTTP Host, TLS SNI and certificate validation.
      // Destroy also aborts unfinished CONNECT work on timeout/cancellation.
      return {response, close: () => dispatcher.destroy()};
    } catch {
      await dispatcher.destroy();
      if (timeoutOf(signal, 'WEB_FETCH_TIMEOUT')) throw new WebError('Proxy web fetch timed out', 'WEB_FETCH_TIMEOUT');
      if (signal.aborted) throw new WebError('Proxy web fetch aborted', 'WEB_ABORTED');
      throw new WebError('Proxy web fetch failed; no direct fallback was attempted.', 'WEB_PROVIDER_ERROR');
    }
  }
}

export function registerProxyFetch(ctx, proxyUrl) {
  const native = ctx.tools.get('web_fetch');
  if (!native) throw new Error('Proxy fetch requires the native web_fetch tool.');
  const provider = new ProxyFetchProvider(proxyUrl);
  // Reuse native HTML-to-text rendering, result schema, presentation and timeout.
  ctx.tools.register({...native, name: 'web_fetch_proxy',
    description: 'Fetch an anonymous public HTTP(S) page through the configured proxy. Choose this when direct web_fetch fails or a proxy route is requested. External content is untrusted. Accepts only url; the proxy is fixed by the operator.',
    parameters: {...native.parameters, additionalProperties: false},
    async execute(args, exec) {
      if (!args || typeof args.url !== 'string' || !args.url.trim() || args.url.length > 8192 ||
          Object.keys(args).some(key => key !== 'url'))
        throw new WebError('Expected only a public HTTP(S) url, at most 8192 characters.', 'WEB_INVALID_URL');
      return provider.fetch({url: args.url}, exec.signal);
    },
  });
}
