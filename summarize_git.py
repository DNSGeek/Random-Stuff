#!/usr/bin/python3

import argparse
import os
from sys import stderr
from typing import Any

import requests

# LM Studio endpoint — adjust if your server runs elsewhere
LLM_URL: str = "http://127.0.0.1:1234/v1/chat/completions"
LLM_MODEL: str = "openai/gpt-oss-20b"

# Max characters of README content to send to the LLM
README_LIMIT: int = 10240


def get_dirs(base_dir: str = ".") -> list[str]:
    """Return a sorted list of non-hidden subdirectory names in base_dir."""
    dirs: list[str] = []
    for name in os.listdir(base_dir):
        if name.startswith("."):
            continue
        # FIX: check isdir AFTER the dot-filter to avoid a stat call on every
        # hidden entry — minor but adds up on repos with many dotfiles.
        if os.path.isdir(os.path.join(base_dir, name)):
            dirs.append(name)
    return sorted(dirs)  # OPT: sort here so callers get a predictable order


def pull_readme(base_dir: str, dir_name: str) -> str:
    """Return the contents of README.md in base_dir/dir_name, or '' if absent."""
    full_dir: str = os.path.join(base_dir, dir_name)
    try:
        entries: list[str] = os.listdir(full_dir)
    except OSError:
        return ""
    for entry in entries:
        if entry.lower() == "readme.md":
            readme_path: str = os.path.join(full_dir, entry)
            try:
                # OPT: read() instead of readlines() + join() — one call, same result.
                # FIX: added encoding='utf-8' — open() in text mode uses the locale
                # default which can vary; many READMEs are UTF-8.
                with open(
                    readme_path, "r", encoding="utf-8", errors="replace"
                ) as f:
                    return f.read()
            except OSError:
                return ""
    return ""


def pull_ai(text: str, session: requests.Session) -> str:
    """Send text to the local LLM and return a 2-3 sentence summary, or '' on failure."""
    # FIX: was building the dict by assigning keys one at a time — use a literal.
    # Also: 'data' was reused for both the request payload AND the response JSON,
    # which is a shadowing bug — after `data = response.json()` the original
    # request dict is gone. Renamed response JSON to `resp_json`.
    # FIX: Union[str, int, float, list[dict[str, str]]] was the type annotation but
    # 'stream': False is a bool and 'max_tokens': -1 is an int — use Any for the
    # value type since this is an open-ended JSON payload.
    payload: dict[str, Any] = {
        "model": LLM_MODEL,
        "temperature": 0.7,
        "max_tokens": -1,
        "stream": False,
        "messages": [
            {
                "role": "system",
                "content": "You are friendly and helpful. You will answer all requests to the best of your ability.",
            },
            {
                "role": "user",
                "content": (
                    "Please write a concise 2 to 3 sentence summary of the following document.\n\n"
                    + text[:README_LIMIT]
                ),
            },
        ],
    }
    try:
        response: requests.Response = session.post(
            LLM_URL,
            # FIX: removed explicit Content-Type header — requests sets it automatically
            # when you pass json=; adding it manually is redundant and error-prone.
            json=payload,
        )
        response.raise_for_status()
        resp_json: dict[str, Any] = response.json()
        return str(resp_json["choices"][0]["message"]["content"])
    except (
        Exception
    ) as ex:  # Yeah, yeah. I know. I don't care what the error was though.
        stderr.write(f"\n\nLLM call failed with error: {ex}\n\n")
        return ""
    # FIX: removed response.close() — requests.Session handles connection pooling;
    # manually closing the response object here is unnecessary and slightly harmful
    # (it prevents keep-alive reuse across the loop iterations).


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Summarize README.md files in subdirectories using a local LLM."
    )
    parser.add_argument(
        "-d", "--directory", help="The base directory to scan", required=True
    )
    args = parser.parse_args()

    basedir: str = args.directory
    summaries: dict[str, str] = {}
    failed: list[str] = []

    # OPT: use a single Session for all requests — keeps the TCP connection alive
    # across calls (already done) and ensures it's properly closed when done.
    with requests.Session() as session:
        for subdir in get_dirs(basedir):
            text: str = pull_readme(basedir, subdir)
            if not text:
                continue
            summary: str = pull_ai(text, session)
            if summary:
                summaries[subdir] = summary
                print(f"{subdir}: {summary}\n")
            else:
                failed.append(subdir)

    # OPT: summaries is already keyed by name; sorted() on a dict iterates sorted keys.
    with open("summaries.txt", "w", encoding="utf-8") as summ:
        for key in sorted(
            summaries
        ):  # OPT: sorted(dict) over sorted(dict.keys()) — idiomatic
            summ.write(f"{key}: {summaries[key]}\n\n")

    if failed:
        print(f"Failed: {sorted(failed)}")
