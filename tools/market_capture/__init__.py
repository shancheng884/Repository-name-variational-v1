"""Read-only multi-venue market capture for RWA research.

This package never imports or calls an order-submission API.  It records public
market data, public account fills (address/API-key read scopes) and local
system/latency evidence so that a later analysis can attribute execution costs.
"""
from .config import DEFAULT_ASSETS, CaptureConfig, load_config
from .model import Book, Fill, ReferenceQuote, adverse_bps, executable_price, spread_bps

__all__ = [
    "Book", "CaptureConfig", "DEFAULT_ASSETS", "Fill", "ReferenceQuote",
    "adverse_bps", "executable_price", "load_config", "spread_bps",
]
