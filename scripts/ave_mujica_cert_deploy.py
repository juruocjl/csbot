#!/usr/bin/python3
"""Validate and atomically publish shared TLS material, without logging secrets."""

import argparse
import fcntl
import grp
import hashlib
import os
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import time


SOURCE_CERT = Path("/var/lib/derper-acme/certificates/ave-mujica.top.crt")
SOURCE_KEY = Path("/var/lib/derper-acme/certificates/ave-mujica.top.key")
TLS_DIR = Path("/var/lib/ave-mujica-tls")
LOCK = Path("/run/lock/ave-mujica-acme.lock")
HOSTS = ("ave-mujica.top", "derp.ave-mujica.top", "www.ave-mujica.top")


def command(args):
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          check=True, timeout=30).stdout


def validate(cert, key, ca_file=None):
    command(["openssl", "x509", "-in", str(cert), "-noout", "-checkend", "86400"])
    san = command(["openssl", "x509", "-in", str(cert), "-noout", "-ext",
                   "subjectAltName"]).decode("ascii")
    names = set(re.findall(r"DNS:([^,\s]+)", san))
    if not {"ave-mujica.top", "*.ave-mujica.top"}.issubset(names):
        raise ValueError("required certificate names missing")
    for host in HOSTS:
        args = ["openssl", "verify", "-purpose", "sslserver", "-verify_hostname", host,
                "-untrusted", str(cert)]
        if ca_file is not None:
            args += ["-CAfile", str(ca_file)]
        command(args + [str(cert)])
    cert_public = command(["openssl", "x509", "-in", str(cert), "-pubkey", "-noout"])
    key_public = command(["openssl", "pkey", "-in", str(key), "-pubout"])
    if cert_public != key_public:
        raise ValueError("certificate and key do not match")


def publish(cert, key, target, gid):
    """Expose the full chain and private key together through one directory link."""
    cert_data, key_data = cert.read_bytes(), key.read_bytes()
    release_name = hashlib.sha256(cert_data).hexdigest()
    releases = target / "releases"
    for directory in (target, releases):
        if directory.is_symlink():
            raise ValueError("certificate directory must not be a symlink")
        directory.mkdir(mode=0o750, parents=True, exist_ok=True)
        os.chmod(directory, 0o750)
        os.chown(directory, -1, gid)
    release = releases / release_name
    if release.is_symlink():
        raise ValueError("certificate release must not be a symlink")
    if not release.exists():
        staging = Path(tempfile.mkdtemp(prefix=".pending-", dir=str(releases)))
        try:
            os.chmod(staging, 0o750)
            os.chown(staging, -1, gid)
            for name, data, mode in (("fullchain.pem", cert_data, 0o644),
                                     ("privkey.pem", key_data, 0o640)):
                file = staging / name
                file.write_bytes(data)
                os.chmod(file, mode)
                os.chown(file, -1, gid)
            (staging / "derp.ave-mujica.top.crt").symlink_to("fullchain.pem")
            (staging / "derp.ave-mujica.top.key").symlink_to("privkey.pem")
            staging.rename(release)
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    if ((release / "fullchain.pem").read_bytes() != cert_data
            or (release / "privkey.pem").read_bytes() != key_data):
        raise ValueError("existing certificate release differs")
    current = target / "current"
    link = target / ".current-new"
    if link.is_symlink():
        link.unlink()
    link.symlink_to("releases/" + release_name)
    os.replace(link, current)
    return release_name


def verify_loaded(host, port):
    expected = command(["openssl", "x509", "-in", str(SOURCE_CERT), "-outform", "DER"])
    context = ssl.create_default_context()
    for attempt in range(5):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=3) as connection:
                with context.wrap_socket(connection, server_hostname=host) as tls:
                    if tls.getpeercert(binary_form=True) != expected:
                        raise ValueError("consumer has not loaded the shared certificate")
            return
        except (OSError, ValueError):
            if attempt == 4:
                raise
            time.sleep(0.5)


def deploy(publish_only=False):
    with LOCK.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        validate(SOURCE_CERT, SOURCE_KEY)
        release = publish(SOURCE_CERT, SOURCE_KEY, TLS_DIR, grp.getgrnam("derper").gr_gid)
        if publish_only:
            print("SHARED_TLS_PUBLISH_RESULT success")
            return
        # Per-consumer markers make failed reloads retryable without reissuing a cert.
        derp_marker = TLS_DIR / "derper-loaded"
        if subprocess.run(["systemctl", "is-active", "--quiet", "derper.service"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          timeout=10).returncode == 0:
            if not (derp_marker.exists() and derp_marker.read_text() == release):
                command(["systemctl", "restart", "derper.service"])
                command(["systemctl", "is-active", "--quiet", "derper.service"])
            verify_loaded("derp.ave-mujica.top", 8443)
            derp_marker.write_text(release)
        nginx_marker = TLS_DIR / "homepage-loaded"
        running = subprocess.run(["docker", "inspect", "--format", "{{.State.Running}}",
                                  "csbot-homepage"], stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, timeout=10).stdout.strip() == b"true"
        if running:
            if not (nginx_marker.exists() and nginx_marker.read_text() == release):
                command(["docker", "exec", "csbot-homepage", "nginx", "-t"])
                command(["docker", "exec", "csbot-homepage", "nginx", "-s", "reload"])
            verify_loaded("ave-mujica.top", 443)
            nginx_marker.write_text(release)
    print("SHARED_TLS_DEPLOY_RESULT success")


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--publish-only", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.check:
            validate(SOURCE_CERT, SOURCE_KEY)
            print("SHARED_TLS_VALIDATION_RESULT success")
        else:
            deploy(publish_only=args.publish_only)
    except (OSError, ValueError, subprocess.SubprocessError):
        print("SHARED_TLS_DEPLOY_RESULT failed")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
