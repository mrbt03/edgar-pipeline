import pytest
import requests

from dags.utils import edgar_fetch

# test the fetch_master_index function success builds correct request
def test_fetch_master_index_success_builds_correct_request(monkeypatch):

    # set the captured variable to an empty dictionary
    captured = {}

    # define a fake get function that captures the url, headers, and timeout
    def fake_get(url, headers=None, timeout=None):
        # capture the url, headers, and timeout
        captured["url"] = url
        captured["headers"] = headers
        captured["timeout"] = timeout

        # define a fake response class that returns a 200 status code and the content "FAKE_IDX"
        class Resp:
            status_code = 200
            content = b"FAKE_IDX"

            def raise_for_status(self):
                return None

        return Resp()

    # Speed up by skipping real sleeps so we don't wait for the network call to complete
    monkeypatch.setattr(edgar_fetch.time, "sleep", lambda _s: None)
    # Intercept network call so we don't hit SEC in test
    monkeypatch.setattr(edgar_fetch.requests, "get", fake_get)

    # fetch the master index
    out = edgar_fetch.fetch_master_index("20240131")
    # assert the output is the expected content
    assert out == b"FAKE_IDX"

    # Correct URL, quarter and filename for 2024-01-31
    assert (
        captured["url"]
        == f"{edgar_fetch.BASE}/2024/QTR1/master.20240131.idx"
    )
    # Production-style headers and timeout
    assert "User-Agent" in captured["headers"]
    assert "edgar-pipeline" in captured["headers"]["User-Agent"]
    assert captured["timeout"] == 30

    # test the fetch_master_index function retries then succeeds by failing first two times and succeeding on the third time
def test_fetch_master_index_retries_then_succeeds(monkeypatch):
    # set the call count to 0
    call_count = {"n": 0}

    # define a fake get function that increments the call count and returns a 200 status code on the third call
    def fake_get(_url, headers=None, timeout=None):
        # increment the call count every time the function is called (should fail first two times and succeed on the third time)
        call_count["n"] += 1

        # define a fake response class that returns a 200 status code on the third call
        class Resp:
            # initialize the response class with the ok parameter
            def __init__(self, ok):
                self._ok = ok
                self.status_code = 200 if ok else 500
                self.content = b"OK" if ok else b""

            # define a method to raise an exception if the status code is not 200
            def raise_for_status(self):
                if not self._ok:
                    raise requests.exceptions.HTTPError("500")

        # Fail first two times, succeed on third
        return Resp(ok=call_count["n"] >= 3)
    # like the previous test, skip the real sleeps so we don't wait for the network call to complete
    monkeypatch.setattr(edgar_fetch.time, "sleep", lambda _s: None)
    # like in the previous test, intercept the network call so we don't hit SEC in test
    monkeypatch.setattr(edgar_fetch.requests, "get", fake_get)

    # fetch the master index
    out = edgar_fetch.fetch_master_index("20240131")
    # assert the output is the expected content
    assert out == b"OK"
    # assert the call count is 3
    assert call_count["n"] == 3

    # test the fetch_master_index function retries then raises an error if the network call fails
def test_fetch_master_index_retries_then_raises(monkeypatch):
    calls = {"n": 0}

    # define a fake get function that always raises a RequestException
    def always_fail(_url, headers=None, timeout=None):
        # increment the call count every time the function is called
        calls["n"] += 1
        # raise a RequestException every time the function is called
        raise requests.exceptions.RequestException("network failure")

    # like in the previous tests, skip the real sleeps so we don't wait for the network call to complete
    monkeypatch.setattr(edgar_fetch.time, "sleep", lambda _s: None)
    # like in the previous tests, intercept the network call so we don't hit SEC in test
    monkeypatch.setattr(edgar_fetch.requests, "get", always_fail)

    # try to fetch the master index and assert it raises a RuntimeError
    with pytest.raises(RuntimeError) as excinfo:
        # fetch the master index
        edgar_fetch.fetch_master_index("20240131")
    # assert the error message contains "Failed to fetch master index"
    assert "Failed to fetch master index" in str(excinfo.value)
    assert calls["n"] == 5

# test the fetch_master_index function raises an error if the date is not 8 characters long
def test_fetch_master_index_invalid_date_length():
    with pytest.raises(ValueError):
        # try to fetch the master index with a date that is not 8 characters long
        edgar_fetch.fetch_master_index("202401") 

# test the fetch_master_index function raises an error if the month is not between 1 and 12
def test_fetch_master_index_invalid_month():
    with pytest.raises(ValueError):
        # try to fetch the master index with a month that is not between 1 and 12
        edgar_fetch.fetch_master_index("20241301") 


