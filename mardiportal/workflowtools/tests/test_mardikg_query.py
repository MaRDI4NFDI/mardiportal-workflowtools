import requests
import pytest
from functools import lru_cache

from mardiportal.workflowtools import mardikg_query


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@lru_cache
def _portal_reachable():
    """Return True if the MaRDI portal API responds within the timeout."""
    try:
        requests.get(
            "https://portal.mardi4nfdi.de/w/api.php",
            params={"action": "query", "meta": "siteinfo", "format": "json"},
            timeout=5,
        )
        return True
    except requests.RequestException:
        return False


def test_query_mardi_kg_for_arxivid_parses_results(monkeypatch):
    """Verify that arXiv ID queries generate the expected search string and parsed results.

    Args:
        monkeypatch: Pytest fixture replacing the HTTP call with a fake implementation.
    """
    captured = {}

    def fake_query(query, **kwargs):
        captured["query"] = query
        return {
            "query": {
                "search": [
                    {
                        "title": "Publication:2176828",
                        "snippet": 'Intro <span class="searchmatch">QIDQ123</span> details',
                    }
                ]
            }
        }

    monkeypatch.setattr(mardikg_query, "query_mardi_kg", fake_query)

    results = mardikg_query.query_mardi_kg_for_arxivid("2104.06175")

    assert captured["query"] == "arXiv2104.06175MaRDI"
    assert results == [
        {
            "qid": "Q123",
            "title": "Publication:2176828",
            "snippet": "Intro QIDQ123 details",
        }
    ]


def test_query_mardi_kg_for_doi_parses_results(monkeypatch):
    """Check that DOI queries use the quoted search string and return normalized results.

    Args:
        monkeypatch: Pytest fixture replacing the HTTP call with a fake implementation.
    """
    captured = {}

    def fake_query(query, **kwargs):
        captured["query"] = query
        return {
            "query": {
                "search": [
                    {
                        "title": "Publication:999",
                        "snippet": 'Value <span class="searchmatch">snippet</span>',
                    }
                ]
            }
        }

    monkeypatch.setattr(mardikg_query, "query_mardi_kg", fake_query)

    results = mardikg_query.query_mardi_kg_for_doi("10.1007/s40305-018-0210-x")

    assert captured["query"] == '"doi.org/10.1007/s40305-018-0210-x"'
    assert results == [
        {
            "qid": "Q999",
            "title": "Publication:999",
            "snippet": "Value snippet",
        }
    ]


def test_query_mardi_kg_retries_then_succeeds(monkeypatch):
    """Ensure the query retries once after a transient failure and ultimately succeeds.

    Args:
        monkeypatch: Pytest fixture stubbing HTTP calls and time.sleep.
    """
    call_count = {"value": 0}

    def fake_post(url, data):
        call_count["value"] += 1
        if call_count["value"] == 1:
            raise requests.RequestException("temporary failure")
        return DummyResponse({"query": {"search": []}})

    monkeypatch.setattr(mardikg_query.requests, "post", fake_post)
    monkeypatch.setattr(mardikg_query.time, "sleep", lambda x: None)

    data = mardikg_query.query_mardi_kg("test-query", max_retries=3, retry_delay=0)

    assert call_count["value"] == 2
    assert data == {"query": {"search": []}}


def test_query_mardi_kg_raises_after_exhausting_retries(monkeypatch, capsys):
    """Confirm the query raises an error after max retries and prints curl context.

    Args:
        monkeypatch: Pytest fixture stubbing dependencies.
        capsys: Pytest fixture capturing stdout for assertions.
    """
    call_count = {"value": 0}

    def fake_post(url, data):
        call_count["value"] += 1
        raise requests.RequestException("persistent failure")

    monkeypatch.setattr(mardikg_query.requests, "post", fake_post)
    monkeypatch.setattr(mardikg_query.time, "sleep", lambda x: None)

    captured_params = {}

    def fake_generate_curl(url, params):
        captured_params["url"] = url
        captured_params["params"] = params
        return "curlcmd"

    monkeypatch.setattr(mardikg_query, "generate_curl_command", fake_generate_curl)

    with pytest.raises(requests.RequestException):
        mardikg_query.query_mardi_kg("final-query", max_retries=2, retry_delay=0)

    assert call_count["value"] == 2
    out = capsys.readouterr().out
    assert "All retries failed." in out
    assert "curlcmd" in out
    assert captured_params["params"]["srsearch"] == "final-query"


@pytest.mark.skipif(
    not _portal_reachable(),
    reason="requires network access to https://portal.mardi4nfdi.de",
)
def test_query_mardi_kg_live_returns_search_payload():
    """Exercise the live MaRDI endpoint using default parameters and validate structure."""
    print("Running 'test_query_mardi_kg_live_returns_search_payload()' ...")
    data = mardikg_query.query_mardi_kg("MaRDI")
    print(f"Got payload with keys: {list(data.keys())}")
    print(f"Search matches: {len(data.get('query', {}).get('search', []))}")
    assert "query" in data
    assert "search" in data["query"]
    assert isinstance(data["query"]["search"], list)


@pytest.mark.skipif(
    not _portal_reachable(),
    reason="requires network access to https://portal.mardi4nfdi.de",
)
def test_query_mardi_kg_for_arxivid_live_returns_results():
    """Hit the live API for a known arXiv identifier and ensure at least one parsed match."""
    print("Running 'test_query_mardi_kg_for_arxivid_live_returns_results()' ...")
    results = mardikg_query.query_mardi_kg_for_arxivid("2104.06175")
    print(f"Retrieved {len(results)} parsed results")
    assert isinstance(results, list)
    assert results  # expect at least one hit for this identifier
    first = results[0]
    print(f"First result: {first}")
    assert {"qid", "title", "snippet"} <= first.keys()
