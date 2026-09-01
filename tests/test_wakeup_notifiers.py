from __future__ import annotations

from types import SimpleNamespace

from tools.lib.wakeup_notifiers import (
    BarkNotifier,
    FeishuUrgentNotifier,
    NotificationResult,
    PushoverNotifier,
    TencentVoiceNotifier,
)


class FakeResponse:
    def __init__(self, body, status_code=200):
        self.body = body
        self.status_code = status_code

    def json(self):
        return self.body


class FakeSession:
    def __init__(self):
        self.posts = []
        self.gets = []
        self.receipt_acknowledged = 0
        self.receipt_expired = 0

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        if url.endswith("/cancel.json"):
            return FakeResponse({"status": 1})
        return FakeResponse({"status": 1, "receipt": "receipt-1"})

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return FakeResponse(
            {
                "status": 1,
                "acknowledged": self.receipt_acknowledged,
                "expired": self.receipt_expired,
            }
        )


def test_pushover_emergency_payload_and_receipt_lifecycle() -> None:
    session = FakeSession()
    notifier = PushoverNotifier(
        app_token="app-secret",
        user_key="user-secret",
        retry_seconds=10,
        expire_seconds=99999,
        session=session,
    )

    sent = notifier.send(title="risk", message="wake up", emergency=True)
    payload = session.posts[0][1]["data"]

    assert sent.ok is True
    assert sent.receipt == "receipt-1"
    assert payload["priority"] == 2
    assert payload["retry"] == 30
    assert payload["expire"] == 10800
    assert "app-secret" not in sent.detail
    assert notifier.receipt_status("receipt-1") == (True, False, "checked")
    session.receipt_acknowledged = 1
    assert notifier.receipt_status("receipt-1") == (True, True, "checked")
    session.receipt_acknowledged = 0
    session.receipt_expired = 1
    assert notifier.receipt_status("receipt-1") == (True, False, "expired")
    assert notifier.cancel("receipt-1").ok is True


def test_tencent_voice_builds_official_request_fields_without_leaking_secrets() -> None:
    captured = {}

    class Client:
        def SendTtsVoice(self, request):
            captured["request"] = request
            return SimpleNamespace(RequestId="request-1")

    def client_factory(secret_id, secret_key, region):
        captured["credentials"] = (secret_id, secret_key, region)
        return Client()

    notifier = TencentVoiceNotifier(
        secret_id="secret-id",
        secret_key="secret-key",
        region="ap-guangzhou",
        sdk_app_id="app-id",
        template_id="template-id",
        called_number="+8613800000000",
        play_times=9,
        client_factory=client_factory,
        request_factory=SimpleNamespace,
    )

    result = notifier.send(["ETH", "保证金风险"])
    request = captured["request"]

    assert result.ok is True
    assert result.detail == "sent:request-1"
    assert request.CalledNumber == "+8613800000000"
    assert request.VoiceSdkAppid == "app-id"
    assert request.TemplateId == "template-id"
    assert request.TemplateParamSet == ["ETH", "保证金风险"]
    assert request.PlayTimes == 3
    assert "secret-key" not in result.detail


def test_bark_critical_push_uses_call_and_does_not_leak_key() -> None:
    class BarkSession:
        def __init__(self):
            self.calls = []

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return FakeResponse({"code": 200, "message": "success"})

    session = BarkSession()
    notifier = BarkNotifier(
        server="https://api.day.app/",
        key="private-device-key",
        sound="minuet",
        volume=20,
        group="risk",
        session=session,
    )

    result = notifier.send(title="risk", message="wake", critical=True)
    url, kwargs = session.calls[0]
    payload = kwargs["json"]

    assert result.ok is True
    assert result.detail == "sent"
    assert url == "https://api.day.app/push"
    assert payload["device_key"] == "private-device-key"
    assert payload["level"] == "critical"
    assert payload["call"] == "1"
    assert payload["volume"] == "10"
    assert payload["isArchive"] == "1"
    assert "private-device-key" not in result.detail


def test_feishu_message_then_phone_urgent_reuses_cached_token() -> None:
    class FeishuSession:
        def __init__(self):
            self.posts = []
            self.patches = []

        def post(self, url, **kwargs):
            self.posts.append((url, kwargs))
            if "tenant_access_token" in url:
                return FakeResponse(
                    {
                        "code": 0,
                        "tenant_access_token": "private-token",
                        "expire": 7200,
                    }
                )
            return FakeResponse(
                {"code": 0, "data": {"message_id": "om_message-1"}}
            )

        def patch(self, url, **kwargs):
            self.patches.append((url, kwargs))
            return FakeResponse(
                {"code": 0, "data": {"invalid_user_id_list": []}}
            )

    session = FeishuSession()
    notifier = FeishuUrgentNotifier(
        app_id="app-id",
        app_secret="private-secret",
        open_id="ou_receiver",
        session=session,
        monotonic=lambda: 100.0,
    )

    message = notifier.send_message(title="risk", message="wake")
    phone = notifier.phone_urgent(message.receipt or "")

    assert message == NotificationResult(True, "sent", "om_message-1")
    assert phone == NotificationResult(True, "sent")
    assert len(session.posts) == 2
    assert len(session.patches) == 1
    assert session.patches[0][0].endswith(
        "/om_message-1/urgent_phone"
    )
    assert session.patches[0][1]["json"] == {
        "user_id_list": ["ou_receiver"]
    }
    assert "private-secret" not in message.detail
    assert "private-token" not in phone.detail
