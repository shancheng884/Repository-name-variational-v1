from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from typing import Any, Callable

import requests


PUSHOVER_MESSAGES_URL = "https://api.pushover.net/1/messages.json"
PUSHOVER_RECEIPTS_URL = "https://api.pushover.net/1/receipts"


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
