from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any
import os

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "log" / "market_capture"

VENUES = ("variational", "lighter_mainnet", "lighter_rh", "entropy")


@dataclass(frozen=True)
class Asset:
    name: str
    symbols: dict[str, str]
    var_instrument_type: str = "perpetual_rwa_future"
    var_kind: str = "equity"

    def symbol(self, venue: str) -> str:
        return self.symbols.get(venue, self.name)


DEFAULT_ASSETS: tuple[Asset, ...] = (
    Asset("OPENAI", {
        "variational": "OPENAI", "lighter_mainnet": "OPENAI",
        "lighter_rh": "OPENAI", "entropy": "io:OAI",
    }),
    Asset("ANTHROPIC", {
        "variational": "ANTHROPIC", "lighter_mainnet": "ANTHROPIC",
        "lighter_rh": "ANTHROPIC", "entropy": "io:ANTH",
    }),
    Asset("NVDA", {
        "variational": "NVDA", "lighter_mainnet": "NVDA",
        "lighter_rh": "NVDA", "entropy": "io:NVDA",
    }),
    Asset("SNDK", {
        "variational": "SNDK", "lighter_mainnet": "SNDK",
        "lighter_rh": "SNDK", "entropy": "io:SNDK",
    }),
    Asset("CRCL", {
        "variational": "CRCL", "lighter_mainnet": "CRCL",
        "lighter_rh": "CRCL", "entropy": "io:CRCL",
    }),
    Asset("AAPL", {
        "variational": "AAPL", "lighter_mainnet": "AAPL",
        "lighter_rh": "AAPL", "entropy": "io:AAPL",
    }),
)


def _clean(value: str | None, default: str = "") -> str:
    return (value if value is not None else default).strip()


def _int_env(env: dict[str, str], name: str, default: int | None = None) -> int | None:
    raw = _clean(env.get(name))
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _bool_env(env: dict[str, str], name: str, default: bool = False) -> bool:
    raw = _clean(env.get(name)).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def read_env_file(path: Path) -> dict[str, str]:
    """Minimal dotenv parser; never logs keys or values."""
    out: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return out
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key:
            out[key] = value
    return out


def load_environment(*, include_private: bool = True) -> dict[str, str]:
    env = dict(os.environ)
    for path in (
        ROOT / ".env",
        Path.home() / ".config" / "market-capture" / "lighter-rh.env",
        Path.home() / ".config" / "market-capture" / "entropy.env",
    ):
        if not include_private and path.name != ".env":
            continue
        for key, value in read_env_file(path).items():
            env.setdefault(key, value)
    return env


@dataclass
class CaptureConfig:
    output_dir: Path = DEFAULT_OUTPUT
    assets: tuple[Asset, ...] = DEFAULT_ASSETS
    venue_enabled: dict[str, bool] = field(default_factory=lambda: {name: True for name in VENUES})
    notional_usd: Decimal = Decimal("20")
    book_snapshot_seconds: float = 5.0
    health_seconds: float = 5.0
    latency_seconds: float = 5.0
    system_seconds: float = 60.0
    fill_poll_seconds: float = 2.0
    edge_seconds: float = 1.0
    var_quote_seconds: float = 15.0
    retention_days: int = 14
    disk_floor_gb: float = 2.0
    max_book_levels: int = 10
    var_ws_url: str = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/prices"
    var_command_url: str = "ws://127.0.0.1:8768"
    lighter_mainnet_rest: str = "https://mainnet.zklighter.elliot.ai"
    lighter_mainnet_ws: str = "wss://mainnet.zklighter.elliot.ai/stream"
    lighter_rh_rest: str = "https://api.rh.lighter.xyz"
    lighter_rh_ws: str = "wss://api.rh.lighter.xyz/stream"
    entropy_info_url: str = "https://api.hyperliquid.xyz/info"
    entropy_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    entropy_dex: str = "io"
    lighter_mainnet_account_index: int | None = None
    lighter_mainnet_api_key_index: int | None = None
    lighter_mainnet_private_key: str = field(default="", repr=False)
    lighter_rh_account_index: int | None = None
    lighter_rh_api_key_index: int | None = None
    lighter_rh_private_key: str = field(default="", repr=False)
    entropy_account_address: str = ""
    duration_seconds: float = 0.0

    def public_dict(self) -> dict[str, Any]:
        return {
            "output_dir": str(self.output_dir),
            "assets": {
                asset.name: dict(asset.symbols) for asset in self.assets
            },
            "venues": dict(self.venue_enabled),
            "notional_usd": str(self.notional_usd),
            "intervals": {
                "book_snapshot_seconds": self.book_snapshot_seconds,
                "health_seconds": self.health_seconds,
                "latency_seconds": self.latency_seconds,
                "system_seconds": self.system_seconds,
                "fill_poll_seconds": self.fill_poll_seconds,
                "edge_seconds": self.edge_seconds,
                "var_quote_seconds": self.var_quote_seconds,
            },
            "retention_days": self.retention_days,
            "disk_floor_gb": self.disk_floor_gb,
            "credentials_present": {
                "lighter_mainnet": bool(
                    self.lighter_mainnet_account_index is not None
                    and self.lighter_mainnet_api_key_index is not None
                    and self.lighter_mainnet_private_key
                ),
                "lighter_rh": bool(
                    self.lighter_rh_account_index is not None
                    and self.lighter_rh_api_key_index is not None
                    and self.lighter_rh_private_key
                ),
                "entropy": bool(self.entropy_account_address),
            },
            "orders_attempted": False,
            "order_submission_enabled": False,
        }


class ConfigError(RuntimeError):
    pass


def _parse_assets(raw: str, env: dict[str, str]) -> tuple[Asset, ...]:
    names = [part.strip().upper() for part in raw.split(",") if part.strip()]
    known = {asset.name: asset for asset in DEFAULT_ASSETS}
    if not names:
        names = ["OPENAI", "ANTHROPIC", "NVDA"]
    out = []
    for name in names:
        if name not in known:
            raise ConfigError(f"unknown_capture_asset:{name}")
        out.append(known[name])
    return tuple(out)


def load_config(args: Any | None = None, env: dict[str, str] | None = None) -> CaptureConfig:
    env = dict(env if env is not None else load_environment())
    if _bool_env(env, "ALLOW_ORDER_SUBMISSION"):
        raise ConfigError("market_capture_refuses_order_submission_environment")
    if not _bool_env(env, "MARKET_CAPTURE_ENABLED") and not getattr(args, "enable_capture", False):
        raise ConfigError("market_capture_disabled_set_MARKET_CAPTURE_ENABLED_or_--enable-capture")

    assets = _parse_assets(getattr(args, "assets", "") or "", env)
    enabled = {name: True for name in VENUES}
    for name in VENUES:
        if getattr(args, f"no_{name}", False):
            enabled[name] = False

    return CaptureConfig(
        output_dir=Path(getattr(args, "output_dir", "") or DEFAULT_OUTPUT).expanduser(),
        assets=assets,
        venue_enabled=enabled,
        notional_usd=Decimal(str(getattr(args, "notional_usd", 20))),
        book_snapshot_seconds=max(0.5, float(getattr(args, "book_snapshot_seconds", 5.0))),
        health_seconds=max(1.0, float(getattr(args, "health_seconds", 5.0))),
        latency_seconds=max(1.0, float(getattr(args, "latency_seconds", 5.0))),
        system_seconds=max(10.0, float(getattr(args, "system_seconds", 60.0))),
        fill_poll_seconds=max(1.0, float(getattr(args, "fill_poll_seconds", 2.0))),
        edge_seconds=max(0.5, float(getattr(args, "edge_seconds", 1.0))),
        var_quote_seconds=max(0.0, float(getattr(args, "var_quote_seconds", 15.0))),
        retention_days=max(1, int(getattr(args, "retention_days", 14))),
        disk_floor_gb=max(0.0, float(getattr(args, "disk_floor_gb", 2.0))),
        max_book_levels=max(1, int(getattr(args, "max_book_levels", 10))),
        var_ws_url=_clean(env.get("VAR_PRICES_WS_URL"), "wss://omni-ws-server.prod.ap-northeast-1.variational.io/prices"),
        var_command_url=_clean(env.get("VAR_COMMAND_URL"), "ws://127.0.0.1:8768"),
        lighter_mainnet_rest=_clean(env.get("LIGHTER_MAINNET_REST_URL"), "https://mainnet.zklighter.elliot.ai").rstrip("/"),
        lighter_mainnet_ws=_clean(env.get("LIGHTER_MAINNET_WS_URL"), "wss://mainnet.zklighter.elliot.ai/stream"),
        lighter_rh_rest=_clean(env.get("LIGHTER_RH_REST_URL"), "https://api.rh.lighter.xyz").rstrip("/"),
        lighter_rh_ws=_clean(env.get("LIGHTER_RH_WS_URL"), "wss://api.rh.lighter.xyz/stream"),
        entropy_info_url=_clean(env.get("ENTROPY_INFO_URL"), "https://api.hyperliquid.xyz/info"),
        entropy_ws_url=_clean(env.get("ENTROPY_WS_URL"), "wss://api.hyperliquid.xyz/ws"),
        entropy_dex=_clean(env.get("ENTROPY_ALLOWED_DEX"), "io"),
        lighter_mainnet_account_index=_int_env(env, "LIGHTER_ACCOUNT_INDEX"),
        lighter_mainnet_api_key_index=_int_env(env, "LIGHTER_API_KEY_INDEX"),
        lighter_mainnet_private_key=_clean(env.get("LIGHTER_PRIVATE_KEY")),
        lighter_rh_account_index=_int_env(env, "LIGHTER_RH_ACCOUNT_INDEX"),
        lighter_rh_api_key_index=_int_env(env, "LIGHTER_RH_API_KEY_INDEX"),
        lighter_rh_private_key=_clean(env.get("LIGHTER_RH_API_PRIVATE_KEY")),
        entropy_account_address=_clean(env.get("ENTROPY_ACCOUNT_ADDRESS")),
        duration_seconds=max(0.0, float(getattr(args, "duration_seconds", 0.0))),
    )
