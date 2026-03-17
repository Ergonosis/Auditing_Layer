"""Shared LLM configuration for all CrewAI agents."""

from crewai import LLM
from src.llm.provider import get_crewai_llm

# Singleton instance for reuse across agents
_agent_llm_instance = None


def get_agent_llm(temperature: float = 0.1) -> LLM:
    """Get configured LLM for agent orchestration."""
    return get_crewai_llm(temperature=temperature)


def get_shared_agent_llm() -> LLM:
    """Get or create shared LLM instance for all agents."""
    global _agent_llm_instance
    if _agent_llm_instance is None:
        _agent_llm_instance = get_agent_llm()
    return _agent_llm_instance
