"""Unit test conftest — registers stub modules for optional SDK dependencies.

graphiti-core ships clients for anthropic and groq as optional extras.
When those SDKs are not installed, the submodules raise ImportError on import.
This conftest registers lightweight stub modules in sys.modules *before* any
test runs, so patch("graphiti_core.llm_client.anthropic_client.AnthropicClient")
resolves correctly without requiring the real SDKs to be installed.
"""

from __future__ import annotations

import sys
import types
import unittest.mock


def _register_sdk_stub(sdk_name: str, **attrs: object) -> None:
    """Put a stub module in sys.modules if the real SDK is not installed."""
    if sdk_name in sys.modules:
        return
    try:
        __import__(sdk_name)
    except ImportError:
        stub = types.ModuleType(sdk_name)
        for name, value in attrs.items():
            setattr(stub, name, value)
        sys.modules[sdk_name] = stub


def _register_graphiti_llm_stub(submodule: str, **attrs: object) -> None:
    """Register a stub for graphiti_core.llm_client.<submodule> if not importable.

    Also sets the stub as an attribute on the graphiti_core.llm_client package
    so that pkgutil.resolve_name (used by unittest.mock.patch) can find it via
    getattr rather than requiring a fresh import.
    """
    full_name = f"graphiti_core.llm_client.{submodule}"
    if full_name in sys.modules:
        # Module is already loaded — nothing to do.
        return
    try:
        __import__(full_name)
    except ImportError:
        stub = types.ModuleType(full_name)
        for name, value in attrs.items():
            setattr(stub, name, value)
        sys.modules[full_name] = stub
        # Register as attribute on the parent package so getattr() finds it.
        import graphiti_core.llm_client as _parent  # already imported (no cost)
        setattr(_parent, submodule, stub)


# ---------------------------------------------------------------------------
# SDK stubs (must come before graphiti submodule stubs)
# ---------------------------------------------------------------------------

_register_sdk_stub("anthropic", Anthropic=unittest.mock.MagicMock)
_register_sdk_stub("groq", Groq=unittest.mock.MagicMock)

# ---------------------------------------------------------------------------
# graphiti_core LLM client submodule stubs
# ---------------------------------------------------------------------------

_register_graphiti_llm_stub(
    "anthropic_client", AnthropicClient=unittest.mock.MagicMock
)
_register_graphiti_llm_stub(
    "groq_client", GroqClient=unittest.mock.MagicMock
)
