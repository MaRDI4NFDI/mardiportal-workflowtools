import re
import requests
from typing import List, Dict
import shlex
import json
import time

def query_mardi_kg_for_arxivid(
        arxiv_id: str, max_retries: int = 5, retry_delay: float = 2.0,
        api_url: str = "https://portal.mardi4nfdi.de/w/api.php",
        namespace="4206") -> List[Dict]:
    """
    Query the MaRDI MediaWiki API for pages mentioning a specific arXiv ID.

    This function queries the MaRDI Knowledge Graph via its MediaWiki API
    for a pattern based on the arXiv ID (formatted as 'arXiv<ID>MaRDI')
    in the 'Publication' namespace.

    Args:
        arxiv_id (str): The arXiv identifier (e.g., "2104.06175").
        max_retries (int, optional): Maximum number of retry attempts on failure.
        retry_delay (float, optional): Delay between retries, in seconds.
        api_url (str, optional): The MediaWiki API endpoint.
        namespace (str, optional): The MediaWiki namespace to search in (default is "4206").

    Returns:
        List[Dict]: A list of search result entries with keys 'qid', 'title', and 'snippet'.
    """

    arxiv_query = f"arXiv{arxiv_id}MaRDI"

    data = query_mardi_kg(
        query=arxiv_query,
        max_retries=max_retries,
        retry_delay=retry_delay,
        api_url=api_url,
        namespace=namespace
    )

    # Extract data from query response
    results = []
    for r in data.get("query", {}).get("search", []):
        snippet = r.get("snippet", "")
        clean_snippet = snippet.replace("<span class=\"searchmatch\">", "").replace("</span>", "")
        qid_match = re.search(r"QID(Q\d+)", clean_snippet)
        qid = qid_match.group(1) if qid_match else None

        results.append({
            "qid": qid,
            "title": r.get("title", "(no title)"),
            "snippet": clean_snippet
        })

    return results


def query_mardi_kg_for_doi(
        doi: str, max_retries: int = 5, retry_delay: float = 2.0,
        api_url: str = "https://portal.mardi4nfdi.de/w/api.php",
        namespace="4206") -> List[Dict]:
    """
    Query the MaRDI MediaWiki API for pages mentioning a specific DOI.

    This function queries the MaRDI Knowledge Graph via its MediaWiki API
    using a quoted DOI string (e.g., '"doi.org/10.1007/s40305-018-0210-x"')
    in the 'Publication' namespace.

    Args:
        doi (str): The DOI identifier (e.g., "10.1007/s40305-018-0210-x").
        max_retries (int, optional): Maximum number of retry attempts on failure.
        retry_delay (float, optional): Delay between retries, in seconds.
        api_url (str, optional): The MediaWiki API endpoint.
        namespace (str, optional): The MediaWiki namespace to search in (default is "4206").

    Returns:
        List[Dict]: A list of search result entries with keys 'qid', 'title', and 'snippet'.
    """

    doi_query = f'"doi.org/{doi}"'

    data = query_mardi_kg(
        query=doi_query,
        max_retries=max_retries,
        retry_delay=retry_delay,
        api_url=api_url,
        namespace=namespace
    )

    # Extract data from query response
    results = []
    for r in data.get("query", {}).get("search", []):
        snippet = r.get("snippet", "")
        clean_snippet = snippet.replace("<span class=\"searchmatch\">", "").replace("</span>", "")

        # Extract QID from the title, which is in the form "Publication:2176828"
        title = r.get("title", "(no title)")
        qid_match = re.match(r"Publication:(\d+)", title)
        qid = f"Q{qid_match.group(1)}" if qid_match else None

        results.append({
            "qid": qid,
            "title": title,
            "snippet": clean_snippet
        })

    return results



def query_mardi_kg(
        query: str, max_retries: int = 5, retry_delay: float = 2.0,
        api_url: str = "https://portal.mardi4nfdi.de/w/api.php", namespace="4206") -> str:
    """
    Perform a raw search query against the MaRDI MediaWiki API.

    This function executes a `list=search` query against the API with retry logic
    in case of network failures or server errors.

    Args:
        query (str): The search string to submit.
        max_retries (int, optional): Maximum number of retry attempts on failure.
        retry_delay (float, optional): Delay between retries, in seconds.
        api_url (str, optional): The MediaWiki API endpoint.
        namespace (str, optional): The MediaWiki namespace to search in.

    Returns:
        dict: The parsed JSON result of the query, containing the search matches.

    Raises:
        requests.RequestException: If all retry attempts fail.
    """

    search_string = query
    params = {
        "action": "query",
        "list": "search",
        "srsearch": search_string,
        "srnamespace": namespace,
        "format": "json"
    }

    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(api_url, data=params)
            response.raise_for_status()
            data = response.json()
            break
        except requests.RequestException as e:
            last_exception = e
            if attempt < max_retries:
                time.sleep(retry_delay)
    else:
        # All retries failed
        print("All retries failed. Curl for debugging:")
        print(generate_curl_command(api_url, params))
        raise last_exception

    return data


def generate_curl_command(url, params=None, json_data=False):
    """
    Generate a curl command equivalent to requests.post().

    Args:
        url (str): The target URL.
        params (dict): Dictionary of data to send.
        json_data (bool): If True, send as JSON; otherwise, as form data.

    Returns:
        str: A curl command string.
    """
    if params is None:
        params = {}

    if json_data:
        data_str = json.dumps(params)
        escaped_data = shlex.quote(data_str)
        curl_cmd = f"curl -X POST {shlex.quote(url)} -H 'Content-Type: application/json' -d {escaped_data}"
    else:
        data_str = '&'.join(f'{k}={v}' for k, v in params.items())
        escaped_data = shlex.quote(data_str)
        curl_cmd = f"curl -X POST {shlex.quote(url)} -d {escaped_data}"

    return curl_cmd


if __name__ == "__main__":
    result = query_mardi_kg_for_arxivid("2104.06175")
    print( result )

    result = query_mardi_kg_for_doi("10.1007/s40305-018-0210-x")
    print( result )
