"""LLM client with automatic cost tracking, retries, and provider abstraction."""

import os
import time
from typing import Optional

from src.llm.provider import (
    get_llm_client,
    _resolve_model,
    MODEL_PRICING,
    ANTHROPIC_FAST_MODEL,
)
from src.utils.metrics import llm_tokens_counter, llm_cost_counter, llm_api_latency
from src.utils.errors import LLMError
from src.utils.logging import get_logger

logger = get_logger(__name__)


def call_llm(
    prompt: str,
    model: Optional[str] = None,
    agent_name: str = "unknown",
    max_retries: int = 3,
) -> str:
    """
    Call LLM with automatic cost tracking and retries.

    Args:
        prompt: User prompt
        model: Model ID (native or legacy — auto-resolved). Defaults to
               DEFAULT_LLM_MODEL env var or Anthropic Haiku 4.5.
        agent_name: Name of calling agent (for metrics labels)
        max_retries: Max retry attempts on transient errors

    Returns:
        LLM response text

    Raises:
        LLMError: If API call fails after all retries
    """
    model = _resolve_model(
        model or os.getenv("DEFAULT_LLM_MODEL", ANTHROPIC_FAST_MODEL)
    )
    client = get_llm_client()

    for attempt in range(max_retries):
        try:
            start_time = time.time()

            text, tokens = client.complete(prompt, model)

            latency = time.time() - start_time
            cost = calculate_cost(tokens, model)

            llm_tokens_counter.labels(model_name=model, agent_name=agent_name).inc(tokens)
            llm_cost_counter.labels(model_name=model).inc(cost)
            llm_api_latency.labels(model_name=model).observe(latency)

            logger.info(
                "LLM call successful",
                model=model,
                tokens=tokens,
                cost=cost,
                latency=latency,
                agent=agent_name,
            )

            return text

        except Exception as e:
            if client.is_rate_limit_error(e):
                logger.warning(f"Rate limit hit, retrying... (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise LLMError(f"Rate limit exceeded after {max_retries} attempts: {e}")
            else:
                logger.error(f"LLM API error: {e}", attempt=attempt)
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise LLMError(f"LLM API call failed after {max_retries} attempts: {e}")


def calculate_cost(tokens: int, model: str) -> float:
    """Calculate cost in USD based on token count and model pricing."""
    model = _resolve_model(model)
    price_per_token = MODEL_PRICING.get(model, 1.00 / 1_000_000)  # default to Haiku rate
    return tokens * price_per_token


def batch_call_llm(prompts: list[str], model: Optional[str] = None) -> list[str]:
    """Batch LLM calls (sequential with shared setup)."""
    return [call_llm(prompt, model) for prompt in prompts]
