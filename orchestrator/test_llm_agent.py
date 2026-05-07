from llm_agent import build_prompt


def _decision(**kwargs):
    base = {
        "transaction_id": "tx-123",
        "account_id": "acc-456",
        "action": "BLOCK",
        "risk_level": "CRITICAL",
        "risk_score": 87.5,
        "reasons": ["high_frequency:8_per_min", "critical_risk_score:87.5"],
    }
    base.update(kwargs)
    return base


def test_prompt_contains_transaction_id():
    prompt = build_prompt(_decision())
    assert "tx-123" in prompt


def test_prompt_contains_account_id():
    prompt = build_prompt(_decision())
    assert "acc-456" in prompt


def test_prompt_contains_action():
    prompt = build_prompt(_decision())
    assert "BLOCK" in prompt


def test_prompt_contains_risk_level():
    prompt = build_prompt(_decision())
    assert "CRITICAL" in prompt


def test_prompt_contains_score():
    prompt = build_prompt(_decision())
    assert "87.5" in prompt


def test_prompt_contains_reasons():
    prompt = build_prompt(_decision())
    assert "high_frequency:8_per_min" in prompt


def test_prompt_empty_reasons_shows_none():
    prompt = build_prompt(_decision(reasons=[]))
    assert "none" in prompt


def test_prompt_allow_decision():
    prompt = build_prompt(_decision(action="ALLOW", risk_level="LOW",
                                    risk_score=12.0, reasons=[]))
    assert "ALLOW" in prompt
    assert "LOW" in prompt
    assert "none" in prompt


def test_prompt_is_string():
    prompt = build_prompt(_decision())
    assert isinstance(prompt, str)
    assert len(prompt) > 50
