from __future__ import annotations

import importlib.util
import json
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import requests


PUSHOVER_MESSAGES_URL = "https://api.pushover.net/1/messages.json"
PUSHOVER_RECEIPTS_URL = "https://api.pushover.net/1/receipts"
DEFAULT_BARK_CONFIG_PATH = (
    Path.home() / ".config" / "var-risk-alarm-a" / "bark.json"
)
DEFAULT_FEISHU_CONFIG_PATH = (
    Path.home() / ".config" / "var-risk-alarm-a" / "feishu.json"
)
FEISHU_TOKEN_URL = (
    "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
)
FEISHU_MESSAGES_URL = "https://open.feishu.cn/open-apis/im/v1/messages"


@dataclass(frozen=True)
class NotificationResult:
    ok: bool
    detail: str
    receipt: str | None = None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _read_private_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def private_config_error(path: Path) -> str | None:
    if not path.exists():
        return "missing"
    if os.name == "nt":
        return None
    try:
        permissions = stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return "permissions_unavailable"
    return "permissions_not_600" if permissions & 0o077 else None


class BarkNotifier:
    def __init__(
        self,
        *,
        server: str,
        key: str,
        sound: str = "alarm",
        volume: int = 10,
        group: str = "Var-Lighter-Risk",
        session: Any = requests,
        config_path: Path | None = None,
    ) -> None:
        self.server = server.strip().rstrip("/")
        self.key = key.strip()
        self.sound = sound.strip() or "alarm"
        self.volume = min(10, max(0, int(volume)))
        self.group = group.strip() or "Var-Lighter-Risk"
        self.session = session
        self.config_path = config_path

    @property
    def enabled(self) -> bool:
        return bool(self.server and self.key)

    @classmethod
    def from_config(cls, path: Path | None = None) -> "BarkNotifier":
        config_path = path or Path(
            os.getenv("BARK_CONFIG_FILE", str(DEFAULT_BARK_CONFIG_PATH))
        ).expanduser()
        body = _read_private_json(config_path)
        return cls(
            server=str(body.get("server") or body.get("base_url") or ""),
            key=str(body.get("key") or body.get("device_key") or ""),
            sound=str(body.get("sound") or "alarm"),
            volume=int(body.get("volume") or 10),
            group=str(body.get("group") or "Var-Lighter-Risk"),
            config_path=config_path,
        )

    def send(
        self,
        *,
        title: str,
        message: str,
        critical: bool,
    ) -> NotificationResult:
        if not self.enabled:
            return NotificationResult(False, "bark_not_configured")
        payload: dict[str, Any] = {
            "device_key": self.key,
            "title": title[:120],
            "body": message[:1800],
            "group": self.group,
            "sound": self.sound,
            "isArchive": "1",
        }
        if critical:
            payload.update(
                {
                    "level": "critical",
                    "volume": str(self.volume),
                    "call": "1",
                }
            )
        try:
            response = self.session.post(
                f"{self.server}/push",
                json=payload,
                timeout=(3.0, 8.0),
            )
        except Exception as exc:
            return NotificationResult(
                False,
                f"bark_request_failed:{type(exc).__name__}",
            )
        if response.status_code != 200:
            return NotificationResult(False, f"bark_http_{response.status_code}")
        try:
            body = response.json()
        except Exception:
            return NotificationResult(False, "bark_invalid_json")
        if int(body.get("code") or 0) != 200:
            return NotificationResult(False, "bark_api_rejected")
        return NotificationResult(True, "sent")


class FeishuUrgentNotifier:
    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        open_id: str,
        session: Any = requests,
        config_path: Path | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.app_id = app_id.strip()
        self.app_secret = app_secret.strip()
        self.open_id = open_id.strip()
        self.session = session
        self.config_path = config_path
        self.monotonic = monotonic
        self._token = ""
        self._token_expires_at = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self.app_id and self.app_secret and self.open_id)

    @classmethod
    def from_config(cls, path: Path | None = None) -> "FeishuUrgentNotifier":
        config_path = path or Path(
            os.getenv("FEISHU_CONFIG_FILE", str(DEFAULT_FEISHU_CONFIG_PATH))
        ).expanduser()
        body = _read_private_json(config_path)
        return cls(
            app_id=str(body.get("app_id") or ""),
            app_secret=str(body.get("app_secret") or ""),
            open_id=str(body.get("open_id") or ""),
            config_path=config_path,
        )

    @staticmethod
    def _body(response: Any, *, prefix: str) -> tuple[dict[str, Any] | None, str]:
        if response.status_code != 200:
            return None, f"{prefix}_http_{response.status_code}"
        try:
            body = response.json()
        except Exception:
            return None, f"{prefix}_invalid_json"
        code = int(body.get("code") or 0)
        if code != 0:
            return None, f"{prefix}_api_{code}"
        return body, "ok"

    def _tenant_token(self) -> tuple[str | None, str]:
        if self._token and self.monotonic() < self._token_expires_at:
            return self._token, "cached"
        try:
            response = self.session.post(
                FEISHU_TOKEN_URL,
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                timeout=(3.0, 8.0),
            )
        except Exception as exc:
            return None, f"feishu_token_failed:{type(exc).__name__}"
        body, detail = self._body(response, prefix="feishu_token")
        if body is None:
            return None, detail
        token = str(body.get("tenant_access_token") or "").strip()
        if not token:
            return None, "feishu_token_missing"
        try:
            expires = int(body.get("expire") or 7200)
        except (TypeError, ValueError):
            expires = 7200
        self._token = token
        self._token_expires_at = self.monotonic() + max(60, expires - 60)
        return token, "fetched"

    def send_message(self, *, title: str, message: str) -> NotificationResult:
        if not self.enabled:
            return NotificationResult(False, "feishu_not_configured")
        token, detail = self._tenant_token()
        if not token:
            return NotificationResult(False, detail)
        text = f"{title}\n{message}"[:4000]
        try:
            response = self.session.post(
                FEISHU_MESSAGES_URL,
                params={"receive_id_type": "open_id"},
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "receive_id": self.open_id,
                    "msg_type": "text",
                    "content": json.dumps(
                        {"text": text},
                        ensure_ascii=False,
                    ),
                },
                timeout=(3.0, 8.0),
            )
        except Exception as exc:
            return NotificationResult(
                False,
                f"feishu_message_failed:{type(exc).__name__}",
            )
        body, detail = self._body(response, prefix="feishu_message")
        if body is None:
            return NotificationResult(False, detail)
        message_id = str((body.get("data") or {}).get("message_id") or "").strip()
        if not message_id:
            return NotificationResult(False, "feishu_message_id_missing")
        return NotificationResult(True, "sent", message_id)

    def phone_urgent(self, message_id: str) -> NotificationResult:
        if not self.enabled or not message_id:
            return NotificationResult(False, "feishu_phone_unavailable")
        token, detail = self._tenant_token()
        if not token:
            return NotificationResult(False, detail)
        try:
            response = self.session.patch(
                f"{FEISHU_MESSAGES_URL}/{message_id}/urgent_phone",
                params={"user_id_type": "open_id"},
                headers={"Authorization": f"Bearer {token}"},
                json={"user_id_list": [self.open_id]},
                timeout=(3.0, 8.0),
            )
        except Exception as exc:
            return NotificationResult(
                False,
                f"feishu_phone_failed:{type(exc).__name__}",
            )
        body, detail = self._body(response, prefix="feishu_phone")
        if body is None:
            return NotificationResult(False, detail)
        invalid = (body.get("data") or {}).get("invalid_user_id_list") or []
        if invalid:
            return NotificationResult(False, "feishu_phone_invalid_receiver")
        return NotificationResult(True, "sent")


class PushoverNotifier:
    def __init__(
        self,
        *,
        app_token: str,
        user_key: str,
        device: str = "",
        retry_seconds: int = 60,
        expire_seconds: int = 1800,
        sound: str = "siren",
        session: Any = requests,
    ) -> None:
        self.app_token = app_token.strip()
        self.user_key = user_key.strip()
        self.device = device.strip()
        self.retry_seconds = max(30, int(retry_seconds))
        self.expire_seconds = min(10800, max(30, int(expire_seconds)))
        self.sound = sound.strip()
        self.session = session

    @property
    def enabled(self) -> bool:
        return bool(self.app_token and self.user_key)

    @classmethod
    def from_env(cls) -> "PushoverNotifier":
        return cls(
            app_token=os.getenv("PUSHOVER_APP_TOKEN", ""),
            user_key=os.getenv("PUSHOVER_USER_KEY", ""),
            device=os.getenv("PUSHOVER_DEVICE", ""),
            retry_seconds=_env_int("PUSHOVER_RETRY_SECONDS", 60),
            expire_seconds=_env_int("PUSHOVER_EXPIRE_SECONDS", 1800),
            sound=os.getenv("PUSHOVER_EMERGENCY_SOUND", "siren"),
        )

    def send(
        self,
        *,
        title: str,
        message: str,
        emergency: bool,
    ) -> NotificationResult:
        if not self.enabled:
            return NotificationResult(False, "pushover_not_configured")
        payload: dict[str, Any] = {
            "token": self.app_token,
            "user": self.user_key,
            "title": title[:250],
            "message": message[:1024],
            "priority": 2 if emergency else 0,
        }
        if self.device:
            payload["device"] = self.device
        if emergency:
            payload.update(
                {
                    "retry": self.retry_seconds,
                    "expire": self.expire_seconds,
                    "sound": self.sound,
                }
            )
        try:
            response = self.session.post(
                PUSHOVER_MESSAGES_URL,
                data=payload,
                timeout=(3.0, 8.0),
            )
        except Exception as exc:
            return NotificationResult(
                False,
                f"pushover_request_failed:{type(exc).__name__}",
            )
        if response.status_code != 200:
            return NotificationResult(
                False,
                f"pushover_http_status_{response.status_code}",
            )
        try:
            body = response.json()
        except Exception:
            return NotificationResult(False, "pushover_invalid_json")
        if int(body.get("status") or 0) != 1:
            return NotificationResult(False, "pushover_api_rejected")
        receipt = str(body.get("receipt") or "").strip() or None
        if emergency and receipt is None:
            return NotificationResult(False, "pushover_missing_receipt")
        return NotificationResult(True, "sent", receipt)

    def receipt_status(self, receipt: str) -> tuple[bool, bool, str]:
        if not self.enabled or not receipt:
            return False, False, "pushover_receipt_unavailable"
        try:
            response = self.session.get(
                f"{PUSHOVER_RECEIPTS_URL}/{receipt}.json",
                params={"token": self.app_token},
                timeout=(3.0, 8.0),
            )
        except Exception as exc:
            return False, False, f"pushover_receipt_failed:{type(exc).__name__}"
        if response.status_code != 200:
            return False, False, f"pushover_receipt_http_{response.status_code}"
        try:
            body = response.json()
        except Exception:
            return False, False, "pushover_receipt_invalid_json"
        if int(body.get("status") or 0) != 1:
            return False, False, "pushover_receipt_rejected"
        acknowledged = bool(int(body.get("acknowledged") or 0))
        expired = bool(int(body.get("expired") or 0))
        return (
            True,
            acknowledged,
            "expired" if expired and not acknowledged else "checked",
        )

    def cancel(self, receipt: str) -> NotificationResult:
        if not self.enabled or not receipt:
            return NotificationResult(False, "pushover_receipt_unavailable")
        try:
            response = self.session.post(
                f"{PUSHOVER_RECEIPTS_URL}/{receipt}/cancel.json",
                data={"token": self.app_token},
                timeout=(3.0, 8.0),
            )
        except Exception as exc:
            return NotificationResult(
                False,
                f"pushover_cancel_failed:{type(exc).__name__}",
            )
        if response.status_code != 200:
            return NotificationResult(
                False,
                f"pushover_cancel_http_{response.status_code}",
            )
        return NotificationResult(True, "cancelled")


class TencentVoiceNotifier:
    def __init__(
        self,
        *,
        secret_id: str,
        secret_key: str,
        region: str,
        sdk_app_id: str,
        template_id: str,
        called_number: str,
        play_times: int = 2,
        client_factory: Callable[[str, str, str], Any] | None = None,
        request_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.secret_id = secret_id.strip()
        self.secret_key = secret_key.strip()
        self.region = region.strip() or "ap-guangzhou"
        self.sdk_app_id = sdk_app_id.strip()
        self.template_id = template_id.strip()
        self.called_number = called_number.strip()
        self.play_times = min(3, max(1, int(play_times)))
        self.client_factory = client_factory
        self.request_factory = request_factory

    @property
    def enabled(self) -> bool:
        return all(
            (
                self.secret_id,
                self.secret_key,
                self.sdk_app_id,
                self.template_id,
                self.called_number,
            )
        )

    @property
    def sdk_available(self) -> bool:
        return importlib.util.find_spec("tencentcloud") is not None

    @classmethod
    def from_env(cls) -> "TencentVoiceNotifier":
        return cls(
            secret_id=os.getenv("TENCENTCLOUD_SECRET_ID", ""),
            secret_key=os.getenv("TENCENTCLOUD_SECRET_KEY", ""),
            region=os.getenv("TENCENT_VMS_REGION", "ap-guangzhou"),
            sdk_app_id=os.getenv("TENCENT_VMS_SDK_APP_ID", ""),
            template_id=os.getenv("TENCENT_VMS_TEMPLATE_ID", ""),
            called_number=os.getenv("TENCENT_VMS_CALLED_NUMBER", ""),
            play_times=_env_int("TENCENT_VMS_PLAY_TIMES", 2),
        )

    @staticmethod
    def _default_client_factory(secret_id: str, secret_key: str, region: str) -> Any:
        from tencentcloud.common import credential
        from tencentcloud.common.profile.client_profile import ClientProfile
        from tencentcloud.common.profile.http_profile import HttpProfile
        from tencentcloud.vms.v20200902 import vms_client

        cred = credential.Credential(secret_id, secret_key)
        http_profile = HttpProfile()
        http_profile.endpoint = "vms.tencentcloudapi.com"
        client_profile = ClientProfile()
        client_profile.httpProfile = http_profile
        return vms_client.VmsClient(cred, region, client_profile)

    def send(self, template_params: list[str]) -> NotificationResult:
        if not self.enabled:
            return NotificationResult(False, "tencent_voice_not_configured")
        try:
            if self.request_factory is None:
                try:
                    from tencentcloud.vms.v20200902 import models
                except ImportError:
                    return NotificationResult(False, "tencentcloud_sdk_missing")
                request = models.SendTtsVoiceRequest()
            else:
                request = self.request_factory()
            factory = self.client_factory or self._default_client_factory
            client = factory(self.secret_id, self.secret_key, self.region)
            request.CalledNumber = self.called_number
            request.VoiceSdkAppid = self.sdk_app_id
            request.TemplateId = self.template_id
            request.TemplateParamSet = [str(value) for value in template_params]
            request.PlayTimes = self.play_times
            response = client.SendTtsVoice(request)
        except Exception as exc:
            return NotificationResult(
                False,
                f"tencent_voice_failed:{type(exc).__name__}",
            )
        request_id = str(getattr(response, "RequestId", "") or "").strip()
        return NotificationResult(
            True,
            f"sent:{request_id}" if request_id else "sent",
        )
