"""Parse Nuxt 3 payload from 500.com HTML pages to extract lottery data."""

import json
import re

import requests


def parse_nuxt_payload(text: str) -> list | None:
    """Extract the Nuxt 3 payload array from HTML."""
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", text, re.DOTALL)
    for s in scripts:
        if "periodicalnum" in s and len(s) > 10000:
            # Find the outer payload array — it starts with [[ at the beginning
            start = s.find("[[")
            if start < 0:
                continue
            # Find matching closing bracket for the outer array
            depth = 0
            in_string = False
            escape = False
            for i, c in enumerate(s[start:], start):
                if escape:
                    escape = False
                    continue
                if c == "\\":
                    escape = True
                    continue
                if c == '"':
                    in_string = not in_string
                    continue
                if in_string:
                    continue
                if c == "[":
                    depth += 1
                elif c == "]":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        try:
                            return json.loads(s[start:end])
                        except json.JSONDecodeError:
                            return None
            break
    return None


def resolve_payload(payload: list) -> dict | None:
    """Resolve Nuxt 3 payload references to extract lottery data."""
    if not isinstance(payload, list) or len(payload) < 2:
        return None

    # Build map of index -> value from the flat array
    values = {}
    for i, item in enumerate(payload):
        values[i] = item

    def resolve_refs(obj, visited=None):
        if visited is None:
            visited = set()
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                if isinstance(v, int) and not isinstance(v, bool):
                    if v in values and v not in visited:
                        visited.add(v)
                        result[k] = resolve_refs(values[v], visited)
                    else:
                        result[k] = v
                else:
                    result[k] = resolve_refs(v, visited)
            return result
        elif isinstance(obj, list):
            return [resolve_refs(item, visited) for item in obj]
        else:
            return obj

    # Find the lotData mapping
    for item in payload:
        if isinstance(item, dict) and "lotData" in item:
            idx = item["lotData"]
            if isinstance(idx, int) and idx in values:
                return resolve_refs(values[idx])
        if isinstance(item, dict) and "periodicalnum" in item:
            return resolve_refs(item)

    return None


def fetch_and_parse_dlt(code: str) -> dict | None:
    """Fetch and parse DLT draw from 500.com."""
    url = f"https://kaijiang.500.com/shtml/dlt/{code}.shtml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    resp = requests.get(url, headers=headers, timeout=60)
    resp.encoding = "utf-8"
    payload = parse_nuxt_payload(resp.text)
    if not payload:
        return None
    return resolve_payload(payload)


def fetch_and_parse_ssq(code: str) -> dict | None:
    """Fetch and parse SSQ draw from 500.com."""
    url = f"https://kaijiang.500.com/shtml/ssq/{code}.shtml"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    resp = requests.get(url, headers=headers, timeout=60)
    resp.encoding = "utf-8"
    payload = parse_nuxt_payload(resp.text)
    if not payload:
        return None
    return resolve_payload(payload)


if __name__ == "__main__":
    # Test with DLT
    data = fetch_and_parse_dlt("26047")
    if data:
        print("=== DLT 26047 ===")
        for k, v in data.items():
            if isinstance(v, (str, int, float)):
                print(f"  {k}: {v}")
    else:
        print("Failed to parse DLT")

    print()
    data = fetch_and_parse_ssq("25048")
    if data:
        print("=== SSQ 25048 ===")
        for k, v in data.items():
            if isinstance(v, (str, int, float)):
                print(f"  {k}: {v}")
    else:
        print("Failed to parse SSQ")
