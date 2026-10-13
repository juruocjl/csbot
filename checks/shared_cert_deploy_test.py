"""Offline checks: real OpenSSL certificates, no ACME or service changes."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "shared_tls", Path(__file__).resolve().parents[1] / "scripts/ave_mujica_cert_deploy.py")
tls = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tls)


class CertificateChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cert, self.key = self.make_cert("shared", "DNS:ave-mujica.top,DNS:*.ave-mujica.top")

    def make_cert(self, name, san):
        cert, key = self.root / (name + ".crt"), self.root / (name + ".key")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-days", "2", "-subj", "/CN=ave-mujica.top", "-addext",
                        "subjectAltName=" + san, "-keyout", str(key), "-out", str(cert)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=True, timeout=30)
        return cert, key

    def test_names_chain_and_matching_key(self):
        tls.validate(self.cert, self.key, ca_file=self.cert)
        with self.assertRaises(subprocess.CalledProcessError):
            tls.validate(self.cert, self.key)

    def test_reject_missing_wildcard_or_mismatched_key(self):
        cert, key = self.make_cert("other", "DNS:ave-mujica.top,DNS:derp.ave-mujica.top")
        with self.assertRaises(ValueError):
            tls.validate(cert, key, ca_file=cert)
        with self.assertRaises(ValueError):
            tls.validate(self.cert, key, ca_file=self.cert)

    def test_atomic_rotation_keeps_previous_pair_and_key_private(self):
        target = self.root / "tls"
        first = tls.publish(self.cert, self.key, target, os.getgid())
        self.assertEqual((target / "current/privkey.pem").stat().st_mode & 0o777, 0o640)
        self.assertEqual((target / "current/derp.ave-mujica.top.crt").read_bytes(), self.cert.read_bytes())
        second_cert, second_key = self.make_cert("second", "DNS:ave-mujica.top,DNS:*.ave-mujica.top")
        second = tls.publish(second_cert, second_key, target, os.getgid())
        self.assertNotEqual(first, second)
        self.assertEqual((target / "releases" / first / "privkey.pem").read_bytes(), self.key.read_bytes())
        self.assertEqual((target / "current/privkey.pem").read_bytes(), second_key.read_bytes())
        self.assertEqual(tls.publish(second_cert, second_key, target, os.getgid()), second)
        (target / "current/privkey.pem").write_bytes(b"altered")
        with self.assertRaises(ValueError):
            tls.publish(second_cert, second_key, target, os.getgid())

    def test_failed_reload_is_retried_without_restarting_derp_again(self):
        target = self.root / "tls"
        operations = []
        fail_reload = [True]

        def fake_command(args):
            operations.append(args)
            if args[-2:] == ["-s", "reload"] and fail_reload[0]:
                fail_reload[0] = False
                raise subprocess.CalledProcessError(1, args)
            return b""

        def fake_probe(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, stdout=b"true\n")

        with patch.multiple(tls, SOURCE_CERT=self.cert, SOURCE_KEY=self.key,
                            TLS_DIR=target, LOCK=self.root / "lock"), \
                patch.object(tls, "validate"), patch.object(tls, "verify_loaded") as verify, \
                patch.object(tls, "command", side_effect=fake_command), \
                patch.object(tls.subprocess, "run", side_effect=fake_probe), \
                patch.object(tls.grp, "getgrnam", return_value=type("Group", (), {"gr_gid": os.getgid()})()):
            with self.assertRaises(subprocess.CalledProcessError):
                tls.deploy()
            self.assertTrue((target / "derper-loaded").exists())
            self.assertFalse((target / "homepage-loaded").exists())
            tls.deploy()
            tls.deploy()
            self.assertTrue((target / "homepage-loaded").exists())
            self.assertIn(unittest.mock.call("ave-mujica.top", 443), verify.call_args_list)
        self.assertEqual(operations.count(["systemctl", "restart", "derper.service"]), 1)
        self.assertEqual(sum(args[-2:] == ["-s", "reload"] for args in operations), 2)


if __name__ == "__main__":
    unittest.main()
