import pytest
from dags.edgar_pipeline import run_ge_checkpoint

def test_ge_checkpoint_stub(monkeypatch):
    # monkeypatch great_expectations to avoid real connection
    class DummyContext:
        def run_checkpoint(self, checkpoint_name):
            return {"success": True}

    monkeypatch.setattr("great_expectations.get_context", lambda **_: DummyContext())

    # Should not raise
    run_ge_checkpoint()
