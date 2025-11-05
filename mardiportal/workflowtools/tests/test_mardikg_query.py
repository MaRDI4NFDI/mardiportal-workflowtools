import requests
import pytest

from mardiportal.workflowtools import mardikg_query


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_query_mardi_kg_for_arxivid_parses_results(monkeypatch):
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
