#!/usr/bin/env python3
"""Minimal allowlist HTTP/CONNECT proxy for Matrix push traffic (stdlib only).

Runs on the GCP host so `cgserver`'s Tuwunel push sync can reach the overseas
push gateways over the encrypted Tailnet instead of a flaky airport node. It is
deliberately not a general proxy: only whitelisted domains on port 443, and
every resolved address is checked *after* DNS so a whitelisted name can never
be rebound onto loopback, RFC1918, link-local, CGNAT metadata or ULA space.

Logs contain only a timestamp, the client IP, the target host:port, the status
code and the byte counts; request lines, headers, tokens and bodies are never
logged.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import ipaddress
import logging
import select
import socket
import socketserver
import sys
from urllib.parse import urlsplit

DEFAULT_ALLOW_DOMAINS = (
    "matrix.org",
    "*.matrix.org",
    "*.push.apple.com",
    "fcm.googleapis.com",
    "firebaseinstallations.googleapis.com",
    "sygnal.matrix.org",
    "api.push.apple.com",
)
DEFAULT_ALLOW_PORTS = frozenset({443})
DEFAULT_ALLOW_CLIENT_CIDRS = ("127.0.0.0/8", "100.64.0.0/10", "::1")
# Explicit SSRF denylist: loopback, RFC1918, link-local/metadata, CGNAT/Tailnet, ULA.
FORBIDDEN_NETWORKS = tuple(ipaddress.ip_network(network) for network in (
    "127.0.0.0/8",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "169.254.0.0/16",
    "100.64.0.0/10",
    "::1/128",
    "fc00::/7",
    "fe80::/10",
))
HOP_BY_HOP_HEADERS = frozenset({
    "connection",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
})
MAX_HEADER_BYTES = 16 * 1024
CONNECT_TIMEOUT = 10.0
RELAY_TIMEOUT = 60.0
STATUS_TEXT = {
    200: "OK",
    400: "Bad Request",
    403: "Forbidden",
    431: "Request Header Fields Too Large",
    502: "Bad Gateway",
}

LOG = logging.getLogger("csbot.matrix_push_proxy")


def normalize_host(host: str) -> str:
    return str(host or "").strip().strip("[]").rstrip(".").lower()


def domain_allowed(host: str, patterns) -> bool:
    """Exact match or a `*.suffix` subdomain match; anything else is denied."""
    name = normalize_host(host)
    if not name:
        return False
    for pattern in patterns or ():
        rule = str(pattern or "").strip().lower().rstrip(".")
        if not rule:
            continue
        if rule.startswith("*."):
            suffix = rule[2:]
            if suffix and name.endswith("." + suffix) and name != suffix:
                return True
        elif name == rule:
            return True
    return False


def _networks(values):
    result = []
    for value in values or ():
        if isinstance(value, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
            result.append(value)
            continue
        try:
            result.append(ipaddress.ip_network(str(value).strip(), strict=False))
        except ValueError:
            continue
    return tuple(result)


def is_forbidden_ip(ip, extra_networks=None) -> bool:
    """True for any address that must never be dialed (fails closed on garbage)."""
    try:
        address = ipaddress.ip_address(str(ip).strip().strip("[]"))
    except ValueError:
        return True
    candidates = [address]
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        candidates.append(mapped)
    for candidate in candidates:
        if (
            candidate.is_loopback
            or candidate.is_private
            or candidate.is_link_local
            or candidate.is_multicast
            or candidate.is_unspecified
            or candidate.is_reserved
        ):
            return True
        for network in FORBIDDEN_NETWORKS + _networks(extra_networks):
            if candidate.version == network.version and candidate in network:
                return True
    return False


def client_allowed(ip, cidrs=DEFAULT_ALLOW_CLIENT_CIDRS) -> bool:
    try:
        address = ipaddress.ip_address(str(ip).strip().strip("[]"))
    except ValueError:
        return False
    mapped = getattr(address, "ipv4_mapped", None)
    if mapped is not None:
        address = mapped
    for network in _networks(cidrs):
        if address.version == network.version and address in network:
            return True
    return False


def parse_connect_target(authority: str) -> tuple[str, int]:
    """Parse `host:port` (or `[v6]:port`) from a CONNECT request line."""
    raw = str(authority or "").strip()
    if not raw:
        raise ValueError("empty CONNECT authority")
    if raw.startswith("["):
        end = raw.find("]")
        if end <= 0:
            raise ValueError("invalid IPv6 authority")
        host = raw[1:end]
        rest = raw[end + 1:]
        if not rest.startswith(":"):
            raise ValueError("CONNECT authority needs host:port")
        port_text = rest[1:]
    else:
        if raw.count(":") != 1:
            raise ValueError("CONNECT authority needs host:port")
        host, port_text = raw.rsplit(":", 1)
    host = host.strip()
    if not host or "/" in host or "@" in host or " " in host or "\x00" in host:
        raise ValueError("invalid CONNECT host")
    if not port_text.isdigit():
        raise ValueError("invalid CONNECT port")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError("invalid CONNECT port")
    return normalize_host(host), port


@dataclass(frozen=True)
class HttpRequestTarget:
    method: str
    host: str
    port: int
    path: str


def parse_proxy_request(method: str, target: str) -> HttpRequestTarget:
    """Parse an absolute-form HTTP proxy request; CONNECT is handled elsewhere."""
    raw = str(target or "").strip()
    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("only absolute HTTP(S) proxy requests are supported")
    if parsed.scheme == "https":
        raise ValueError("HTTPS requires a CONNECT tunnel")
    try:
        port = parsed.port or 80
    except ValueError:
        raise ValueError("invalid proxy target port") from None
    host = normalize_host(parsed.hostname or "")
    if not host:
        raise ValueError("invalid proxy target host")
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    return HttpRequestTarget(method.upper(), host, port, path)


def resolve_addresses(host: str, port: int, *, resolver=None) -> tuple[str, ...]:
    try:
        infos = (resolver or socket.getaddrinfo)(host, port, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError):
        return ()
    addresses: list[str] = []
    for info in infos:
        address = info[4][0]
        if address not in addresses:
            addresses.append(address)
    return tuple(addresses)


@dataclass(frozen=True)
class TargetDecision:
    status: int
    host: str
    port: int
    addresses: tuple[str, ...] = ()
    reason: str = ""


def evaluate_target(
    host: str,
    port: int,
    *,
    allow_domains=DEFAULT_ALLOW_DOMAINS,
    allow_ports=DEFAULT_ALLOW_PORTS,
    resolver=None,
    extra_forbidden=None,
) -> TargetDecision:
    """Domain/port allowlist plus post-DNS address validation (fail closed)."""
    host = normalize_host(host)
    if not domain_allowed(host, allow_domains):
        return TargetDecision(403, host, port, (), "domain not allowed")
    if port not in allow_ports:
        return TargetDecision(403, host, port, (), "port not allowed")
    addresses = resolve_addresses(host, port, resolver=resolver)
    if not addresses:
        return TargetDecision(502, host, port, (), "dns resolution failed")
    for address in addresses:
        if is_forbidden_ip(address, extra_forbidden):
            return TargetDecision(403, host, port, addresses, "forbidden target address")
    return TargetDecision(200, host, port, addresses)


def evaluate_connect_target(authority: str, **kwargs) -> TargetDecision:
    try:
        host, port = parse_connect_target(authority)
    except ValueError as exc:
        return TargetDecision(400, "", 0, (), str(exc))
    return evaluate_target(host, port, **kwargs)


def handle_connect_target(authority: str, **kwargs) -> int:
    """Return the HTTP status the proxy would answer (200 allows the tunnel)."""
    return evaluate_connect_target(authority, **kwargs).status


def _open_target(decision: TargetDecision) -> socket.socket:
    last_error: OSError | None = None
    for address in decision.addresses:
        family = socket.AF_INET6 if ":" in address else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT)
        try:
            endpoint = (address, decision.port, 0, 0) if family == socket.AF_INET6 else (address, decision.port)
            sock.connect(endpoint)
            return sock
        except OSError as exc:
            last_error = exc
            sock.close()
    raise OSError("unable to connect to the approved target") from last_error


def _relay(client: socket.socket, upstream: socket.socket) -> tuple[int, int]:
    """Copy bytes both ways until EOF/timeout; return (sent, received)."""
    sent = received = 0
    peers = (client, upstream)
    try:
        while True:
            readable, _, _ = select.select(peers, (), (), RELAY_TIMEOUT)
            if not readable:
                break
            for source in readable:
                chunk = source.recv(65536)
                if not chunk:
                    return sent, received
                destination = upstream if source is client else client
                destination.sendall(chunk)
                if source is client:
                    sent += len(chunk)
                else:
                    received += len(chunk)
    except OSError:
        pass
    return sent, received


def forward_headers(headers, host: str, port: int) -> bytes:
    lines = []
    for name, value in headers:
        if name.lower() in HOP_BY_HOP_HEADERS:
            continue
        lines.append(f"{name}: {value}\r\n")
    host_value = host if port in (80, 443) else f"{host}:{port}"
    return ("Host: " + host_value + "\r\n" + "".join(lines) + "\r\n").encode("latin-1")


def log_access(client_ip: str, host: str, port: int, status: int,
               sent: int = 0, received: int = 0) -> None:
    # Fixed fields only: never request lines, headers, tokens or bodies.
    LOG.info(
        "client=%s target=%s:%s status=%s sent=%s received=%s",
        client_ip, host or "-", port or "-", status, sent, received,
    )


class ProxyRequestHandler(socketserver.StreamRequestHandler):
    server: "ProxyServer"
    # Unbuffered reads: a read-ahead buffer would swallow the client's first TLS
    # bytes after the CONNECT header block and break the tunnel.
    rbufsize = 0

    def handle(self) -> None:
        client_ip = str(self.client_address[0])
        try:
            status, host, port, sent, received = self._serve(client_ip)
        except Exception:  # never echo exception text (it may contain targets)
            status, host, port, sent, received = 502, "-", 0, 0, 0
            self._safe_status(502)
        log_access(client_ip, host, port, status, sent, received)

    def _safe_status(self, status: int) -> None:
        try:
            self._write_status(status)
        except OSError:
            pass

    def _write_status(self, status: int) -> None:
        reason = STATUS_TEXT.get(status, "Error")
        self.connection.sendall(
            f"HTTP/1.1 {status} {reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".encode("latin-1")
        )

    def _read_headers(self):
        headers = []
        total = 0
        while True:
            line = self.rfile.readline(MAX_HEADER_BYTES + 1)
            total += len(line)
            if not line or total > MAX_HEADER_BYTES:
                raise ValueError("header block too large")
            if line in (b"\r\n", b"\n"):
                return headers
            text = line.decode("latin-1").rstrip("\r\n")
            if ":" not in text:
                continue
            name, value = text.split(":", 1)
            headers.append((name.strip(), value.strip()))

    def _serve(self, client_ip: str):
        if not client_allowed(client_ip, self.server.allow_client_cidrs):
            self._write_status(403)
            return 403, "-", 0, 0, 0
        line = self.rfile.readline(MAX_HEADER_BYTES + 1)
        if not line or len(line) > MAX_HEADER_BYTES:
            self._write_status(400)
            return 400, "-", 0, 0, 0
        parts = line.decode("latin-1").split()
        if len(parts) != 3 or not parts[2].startswith("HTTP/"):
            self._write_status(400)
            return 400, "-", 0, 0, 0
        method, target = parts[0].upper(), parts[1]
        try:
            headers = self._read_headers()
        except ValueError:
            self._write_status(431)
            return 431, "-", 0, 0, 0
        if method == "CONNECT":
            decision = evaluate_connect_target(
                target,
                allow_domains=self.server.allow_domains,
                allow_ports=self.server.allow_ports,
                resolver=self.server.resolver,
                extra_forbidden=self.server.extra_forbidden,
            )
            if decision.status != 200:
                self._write_status(decision.status)
                return decision.status, decision.host or "-", decision.port, 0, 0
            upstream = _open_target(decision)
            try:
                self.connection.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                sent, received = _relay(self.connection, upstream)
            finally:
                upstream.close()
            return 200, decision.host, decision.port, sent, received
        try:
            request = parse_proxy_request(method, target)
        except ValueError:
            self._write_status(400)
            return 400, "-", 0, 0, 0
        if any(
            name.lower() == "transfer-encoding"
            or (name.lower() == "content-length" and value.strip() not in {"", "0"})
            for name, value in headers
        ):
            # A body would already be partly buffered in rfile; refuse instead of
            # silently forwarding a truncated request.
            self._write_status(400)
            return 400, request.host, request.port, 0, 0
        decision = evaluate_target(
            request.host,
            request.port,
            allow_domains=self.server.allow_domains,
            allow_ports=self.server.allow_ports,
            resolver=self.server.resolver,
            extra_forbidden=self.server.extra_forbidden,
        )
        if decision.status != 200:
            self._write_status(decision.status)
            return decision.status, request.host, request.port, 0, 0
        upstream = _open_target(decision)
        try:
            head = f"{request.method} {request.path} HTTP/1.1\r\n".encode("latin-1")
            upstream.sendall(head + forward_headers(headers, request.host, request.port))
            sent, received = _relay(self.connection, upstream)
        finally:
            upstream.close()
        return 200, request.host, request.port, sent, received


class ProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        address,
        handler=ProxyRequestHandler,
        *,
        allow_domains=DEFAULT_ALLOW_DOMAINS,
        allow_ports=DEFAULT_ALLOW_PORTS,
        allow_client_cidrs=DEFAULT_ALLOW_CLIENT_CIDRS,
        resolver=None,
        extra_forbidden=None,
    ):
        self.allow_domains = tuple(allow_domains)
        self.allow_ports = frozenset(allow_ports)
        self.allow_client_cidrs = tuple(allow_client_cidrs)
        self.resolver = resolver
        self.extra_forbidden = tuple(extra_forbidden or ())
        super().__init__(address, handler)


def bind_host(value: str) -> str:
    host = str(value or "").strip()
    if not host:
        raise argparse.ArgumentTypeError("a bind host is required")
    address = host.strip("[]")
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        if address in {"", "*", "0.0.0.0", "::"}:
            raise argparse.ArgumentTypeError("refusing to bind a wildcard address") from None
        return host
    if parsed.is_unspecified:
        raise argparse.ArgumentTypeError("refusing to bind a wildcard address")
    return host


def port_number(value: str) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError("port must be an integer") from None
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def domain_pattern(value: str) -> str:
    pattern = str(value or "").strip().lower()
    if not pattern or "/" in pattern or "://" in pattern or " " in pattern:
        raise argparse.ArgumentTypeError("invalid domain pattern")
    if pattern.startswith("*.") and len(pattern) <= 2:
        raise argparse.ArgumentTypeError("invalid domain pattern")
    return pattern


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, type=bind_host,
                        help="bind address; wildcard addresses are refused")
    parser.add_argument("--port", type=port_number, default=18080)
    parser.add_argument("--allow-domain", action="append", default=[], type=domain_pattern,
                        metavar="DOMAIN", help="extra allowlist entry (exact or *.suffix)")
    parser.add_argument("--allow-client-cidr", action="append", default=[],
                        metavar="CIDR", help="extra allowed client source CIDR")
    args = parser.parse_args(argv)
    domains = list(DEFAULT_ALLOW_DOMAINS)
    for pattern in args.allow_domain:
        if pattern not in domains:
            domains.append(pattern)
    cidrs = list(DEFAULT_ALLOW_CLIENT_CIDRS)
    for cidr in args.allow_client_cidr:
        try:
            ipaddress.ip_network(str(cidr).strip(), strict=False)
        except ValueError:
            parser.error("invalid --allow-client-cidr value")
        if cidr not in cidrs:
            cidrs.append(cidr)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
    server = ProxyServer(
        (args.host, args.port),
        allow_domains=tuple(domains),
        allow_client_cidrs=tuple(cidrs),
    )
    LOG.info("listen=%s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
