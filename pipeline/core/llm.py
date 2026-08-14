"""Narrative text generation via Groq's OpenAI-compatible chat API.

Config: GROQ_API_KEY (required), GROQ_MODEL, GROQ_BASE_URL.
Failures are non-fatal — a placeholder string is returned so reports still build.
"""
import logging
import os

import requests

log = logging.getLogger(__name__)

DEFAULT_MODEL = "llama-3.3-70b-versatile"
DEFAULT_BASE_URL = "https://api.groq.com/openai/v1"


def generate_narrative(prompt, max_tokens=300):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        log.warning("GROQ_API_KEY not set — returning placeholder text")
        return "[Narrative not generated — GROQ_API_KEY missing]"
    base = os.getenv("GROQ_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    model = os.getenv("GROQ_MODEL", DEFAULT_MODEL)
    try:
        r = requests.post(
            f"{base}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.4,
            },
            timeout=120,
        )
        r.raise_for_status()
        choice = (r.json().get("choices") or [{}])[0]
        if choice.get("finish_reason") == "length":
            log.warning(f"Narrative response hit max_tokens={max_tokens} — output likely truncated")
        txt = (choice.get("message", {}).get("content") or "").strip()
        return txt or "[Narrative not generated — empty response]"
    except Exception as e:
        log.warning(f"Narrative generation failed (non-fatal): {e}")
        return "[Narrative not generated — LLM error]"
