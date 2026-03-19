"""Provider-agnostic LLM abstraction. All LLM calls route through this module."""

import os
from typing import Optional

import anthropic
import openai
from crewai import LLM

from src.utils.logging import get_logger

logger = get_logger(__name__)

# ── Model constants ──────────────────────────────────────────────────────────
ANTHROPIC_FAST_MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_SMART_MODEL = "claude-sonnet-4-6"
OPENAI_FAST_MODEL = "gpt-4.1-mini-2025-04-14"
OPENAI_SMART_MODEL = "gpt-4.1-2025-04-14"

# Pricing per token (input, simplified — used for cost tracking)
MODEL_PRICING = {
    ANTHROPIC_FAST_MODEL: 1.00 / 1_000_000,
    ANTHROPIC_SMART_MODEL: 3.00 / 1_000_000,
    OPENAI_FAST_MODEL: 0.40 / 1_000_000,
    OPENAI_SMART_MODEL: 2.00 / 1_000_000,
}

# Legacy OpenRouter-style model names → native IDs
_LEGACY_MODEL_MAP = {
    "anthropic/claude-haiku-4.5": ANTHROPIC_FAST_MODEL,
    "anthropic/claude-sonnet-4.5": ANTHROPIC_SMART_MODEL,
    "anthropic/claude-sonnet-4": ANTHROPIC_SMART_MODEL,
    "openai/gpt-4o-mini": OPENAI_FAST_MODEL,
    "openai/gpt-4o": OPENAI_SMART_MODEL,
}


def _resolve_model(model: str) -> str:
    """Map legacy OpenRouter-style names to native provider model IDs."""
    resolved = _LEGACY_MODEL_MAP.get(model)
    if resolved:
        logger.warning(f"Deprecated model name '{model}' mapped to '{resolved}'. Update your config.")
        return resolved
    return model


def get_provider() -> str:
    """Return the active LLM provider name."""
    return os.getenv("LLM_PROVIDER", "anthropic").lower()


# ── Provider client ──────────────────────────────────────────────────────────
class LLMClient:
    """Thin wrapper around provider SDK. Single mock target for tests."""

    def __init__(self):
        self.provider = get_provider()
        if self.provider == "anthropic":
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if not api_key:
                raise ValueError("ANTHROPIC_API_KEY environment variable is not set")
            self._client = anthropic.Anthropic(api_key=api_key)
        elif self.provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable is not set")
            self._client = openai.OpenAI(api_key=api_key)
        else:
            raise ValueError(f"Unknown LLM_PROVIDER: {self.provider}")

    def complete(self, prompt: str, model: str, max_tokens: int = 4096) -> tuple[str, int]:
        """
        Send a prompt and return (response_text, total_tokens).

        Args:
            prompt: User message content
            model: Native provider model ID (legacy names auto-resolved)
            max_tokens: Max output tokens
        """
        model = _resolve_model(model)

        if self.provider == "anthropic":
            resp = self._client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            tokens = resp.usage.input_tokens + resp.usage.output_tokens
            return resp.content[0].text, tokens

        else:  # openai
            resp = self._client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content, resp.usage.total_tokens

    def is_rate_limit_error(self, exc: Exception) -> bool:
        """Check if an exception is a rate-limit (429) error for the active provider."""
        if self.provider == "anthropic":
            return isinstance(exc, anthropic.APIStatusError) and exc.status_code == 429
        else:
            return isinstance(exc, openai.RateLimitError)


# ── Singleton ────────────────────────────────────────────────────────────────
_client_instance: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Get or create the singleton LLM client."""
    global _client_instance
    if _client_instance is None:
        _client_instance = LLMClient()
    return _client_instance


def reset_llm_client():
    """Reset singleton (for tests)."""
    global _client_instance
    _client_instance = None


# ── CrewAI LLM (agent orchestration) ────────────────────────────────────────
def get_crewai_llm(temperature: float = 0.1) -> LLM:
    """
    Return a CrewAI LLM instance configured for the active provider.

    Uses the provider/model prefix (e.g. "anthropic/claude-sonnet-4-6")
    which routes to the native Anthropic SDK in CrewAI 1.x.
    """
    provider = get_provider()

    if provider == "anthropic":
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is not set")
        model = f"anthropic/{ANTHROPIC_SMART_MODEL}"
        llm = LLM(
            model=model,
            api_key=api_key,
            temperature=temperature,
            max_tokens=4096,
        )
    elif provider == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")
        llm = LLM(
            model=OPENAI_SMART_MODEL,
            api_key=api_key,
            temperature=temperature,
            max_tokens=4096,
        )
    else:
        raise ValueError(f"Unknown LLM_PROVIDER: {provider}")

    logger.info(f"Initialized CrewAI LLM: {llm.model} (provider={provider}, temp={temperature})")
    return llm
