from __future__ import annotations

from types import SimpleNamespace

from tools.lib.wakeup_notifiers import PushoverNotifier, TencentVoiceNotifier


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
