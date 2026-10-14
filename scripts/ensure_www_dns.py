#!/usr/bin/python3
"""Inspect or add the fixed www/cs CNAMEs using inherited AliDNS credentials."""

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid


DOMAIN = "ave-mujica.top"


class APIError(Exception):
    pass


def api(action, parameters):
    access = os.environ["ALICLOUD_ACCESS_KEY"]
    secret = os.environ["ALICLOUD_SECRET_KEY"]
    params = {
        "Action": action, "AccessKeyId": access, "Version": "2015-01-09",
        "Format": "JSON", "SignatureMethod": "HMAC-SHA1", "SignatureVersion": "1.0",
        "SignatureNonce": str(uuid.uuid4()),
        "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **parameters,
    }
    encode = lambda value: urllib.parse.quote(str(value), safe="~")
    canonical = "&".join(encode(k) + "=" + encode(v) for k, v in sorted(params.items()))
    to_sign = "GET&%2F&" + encode(canonical)
    signature = base64.b64encode(hmac.new((secret + "&").encode(), to_sign.encode(),
                                        hashlib.sha1).digest()).decode()
    url = "https://alidns.aliyuncs.com/?" + canonical + "&Signature=" + encode(signature)
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        try:
            code = json.load(error).get("Code", "UNKNOWN")
        except (ValueError, OSError):
            code = "UNKNOWN"
        # Never log signed URLs, SDK diagnostics or response messages.
        raise APIError(code if re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", str(code)) else "UNKNOWN") from None
    if "Code" in result:
        raise APIError("API_RESPONSE_ERROR")
    return result


def records(label="www"):
    result = api("DescribeDomainRecords", {"DomainName": DOMAIN, "RRKeyWord": label,
                                           "PageSize": "500", "PageNumber": "1"})
    if int(result["TotalCount"]) > 500:
        raise ValueError("record query incomplete")
    return [record for record in result.get("DomainRecords", {}).get("Record", [])
            if record.get("RR", "").lower() == label]


def expected(records):
    return (len(records) == 1 and records[0].get("Type") == "CNAME"
            and records[0].get("Value", "").rstrip(".").lower() == DOMAIN
            and records[0].get("Status", "").upper() == "ENABLE")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", choices=("www", "cs"), default="www")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-dir", type=Path)
    args = parser.parse_args()
    prefix = args.label.upper() + "_DNS"
    os.umask(0o077)
    try:
        before = records(args.label)
        print(prefix + "_RECORD_COUNT=" + str(len(before)))
        for record in before:
            view = {key: record.get(key) for key in ("RR", "Type", "Status")}
            if record.get("Type") in ("A", "AAAA", "CNAME"):
                view["Value"] = record.get("Value")
            print(json.dumps(view))
        if expected(before):
            print(prefix + "_RESULT already_configured")
            return
        if not args.apply:
            print(prefix + "_RESULT not_configured")
            return
        if before:
            raise APIError("EXISTING_" + args.label.upper() + "_RECORD_CONFLICT")
        if args.backup_dir is None:
            raise APIError("BACKUP_DIRECTORY_REQUIRED")
        args.backup_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        before_file = args.backup_dir / ("dns-" + args.label + "-before.json")
        with before_file.open("x") as file:
            json.dump({"domain": DOMAIN, "records": before}, file)
        api("AddDomainRecord", {"DomainName": DOMAIN, "RR": args.label, "Type": "CNAME",
                                "Value": DOMAIN, "TTL": "600", "Line": "default"})
        after = records(args.label)
        (args.backup_dir / ("dns-" + args.label + "-after.json")).write_text(json.dumps(after))
        if not expected(after):
            raise APIError("RECORD_VERIFICATION_FAILED")
        print(prefix + "_RESULT created_CNAME_to_" + DOMAIN)
    except APIError as error:
        print(prefix + "_RESULT failed code=" + str(error))
        raise SystemExit(1)
    except (OSError, ValueError, KeyError):
        print(prefix + "_RESULT failed code=REQUEST_OR_CONFIGURATION_ERROR")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
