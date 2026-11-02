"""Narrow, local Chromium session reader for one first-party GET endpoint.

No session secrets are persisted, logged, accepted in arguments or sent through
redirects. Unsupported cookie protection requires normal browser re-login;
this module never changes browser security settings or refreshes a page.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3

import requests

ORIGIN = "https://omni.variational.io"
HOSTS = ("omni.variational.io", ".variational.io")
COOKIE_NAMES = ("vr-token", "vr-ll-token", "cf_clearance", "__cf_bm", "_cfuvid")


def decode_cookie(host, encrypted):
    from Crypto.Cipher import AES
    if len(encrypted) < 19 or not encrypted.startswith(b"v10") or (len(encrypted)-3) % 16:
        raise ValueError("var_session_cookie_protection_unsupported")
    key = hashlib.pbkdf2_hmac("sha1", b"peanuts", b"saltysalt", 1, 16)
    raw = AES.new(key, AES.MODE_CBC, iv=b" "*16).decrypt(encrypted[3:])
    padding = raw[-1]
    if not 1 <= padding <= 16 or raw[-padding:] != bytes([padding])*padding:
        raise ValueError("var_session_cookie_decode_failed")
    raw = raw[:-padding]
    if raw[:32] != hashlib.sha256(host.encode()).digest():
        raise ValueError("var_session_cookie_origin_mismatch")
    return raw[32:].decode("utf-8")


def session_cookies(cookie_db: Path, *, now):
    cookie_db = Path(cookie_db)
    if not cookie_db.is_absolute() or cookie_db.is_symlink():
        raise ValueError("var_session_cookie_path_invalid")
    jar = requests.cookies.RequestsCookieJar()
    db = sqlite3.connect(cookie_db.as_uri()+"?mode=ro", uri=True, timeout=1)
    try:
        version = db.execute("select value from meta where key='version'").fetchone()
        if not version or int(version[0]) < 24:
            raise ValueError("var_session_cookie_schema_unsupported")
        row = db.execute("select host_key,encrypted_value,expires_utc from cookies "
            "where host_key=? and name=?", (HOSTS[0], "vr-connected-address")).fetchone()
        if row is None:
            raise ValueError("var_session_wallet_missing")
        address = decode_cookie(row[0], row[1])
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
            raise ValueError("var_session_wallet_invalid")
        names = (*COOKIE_NAMES, "vr-ll-token-"+address.lower(), "vr-connected-address")
        rows = db.execute("select host_key,name,encrypted_value,expires_utc from cookies "
            "where host_key in (?,?) and name in ("+",".join("?" for _ in names)+")",
            (*HOSTS, *names)).fetchall()
        for host, name, encrypted, expiry in rows:
            if expiry and expiry/1_000_000-11_644_473_600 <= now.timestamp():
                continue
            jar.set(name, decode_cookie(host, encrypted), domain=host, path="/", secure=True)
    finally:
        db.close()
    if not any(c.name == "vr-token" and c.domain == HOSTS[0] for c in jar):
        raise ValueError("var_session_auth_missing_or_expired")
    return address, jar


def fetch_fee_capture(state, cookie_db, *, now=None, session_factory=requests.Session):
    now = now or datetime.now(timezone.utc)
    from tools.lib.wti_exit_cost import fingerprint
    fingerprint(state)
    position = state["open_position"]
    ids = {x["entry_execution_id"] for x in position.get("layers", [position])}
    opened = min(datetime.fromisoformat(e["intent_at"]) for e in state["execution_history"]
                 if e["action_id"] in ids)
    address, jar = session_cookies(Path(cookie_db), now=now)
    params = {"type": "fee", "limit": 100, "offset": 0, "order_by": "created_at", "order": "desc",
              "created_at_gte": (opened-timedelta(seconds=5)).isoformat(), "created_at_lte": now.isoformat()}
    url = requests.Request("GET", ORIGIN+"/api/transfers", params=params).prepare().url
    version_file = Path(cookie_db).parent.parent/"Last Version"
    version = version_file.read_text().strip() if version_file.exists() else "140.0.0.0"
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+){3}", version):
        raise ValueError("var_session_browser_version_invalid")
    user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"+version+" Safari/537.36"
    with session_factory() as session:
        session.trust_env = False
        with session.get(url, cookies=jar, headers={"vr-connected-address": address,
                         "User-Agent": user_agent}, timeout=(3, 10), allow_redirects=False, stream=True) as response:
            if response.status_code != 200:
                raise ValueError("var_fee_http_"+str(response.status_code))
            parts, size = [], 0
            for part in response.iter_content(65536):
                size += len(part)
                if size > 2*1024*1024:
                    raise ValueError("var_fee_response_too_large")
                parts.append(part)
            text = b"".join(parts).decode("utf-8")
            json.loads(text)
    # The receipt deliberately excludes Cookie, Authorization and Set-Cookie.
    return {"log": {"entries": [{"startedDateTime": now.isoformat(),
        "request": {"method": "GET", "url": url,
                    "headers": [{"name": "vr-connected-address", "value": address}]},
        "response": {"status": 200, "content": {"text": text}}}]}}
