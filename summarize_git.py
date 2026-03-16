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

# Seconds to wait for the LLM before giving up
LLM_TIMEOUT: int = 60


def get_dirs(base_dir: str = ".") -> list[str]:
    """Return a sorted list of non-hidden subdirectory names in base_dir."""
    dirs: list[str] = []
    with os.scandir(base_dir) as it:
        for entry in it:
            if not entry.name.startswith(".") and entry.is_dir():
                dirs.append(entry.name)
    return sorted(dirs)


def pull_readme(base_dir: str, dir_name: str) -> str:
    """Return the (possibly truncated) contents of README.md in base_dir/dir_name, or '' if absent."""
    full_dir: str = os.path.join(base_dir, dir_name)
    try:
        entries: list[str] = os.listdir(full_dir)
    except OSError:
        return ""

    for entry in entries:
        if entry.lower() == "readme.md":
            readme_path: str = os.path.join(full_dir, entry)
            try:
                with open(readme_path, "r", encoding="utf-8", errors="replace") as f:
                    content: str = f.read()
                if len(content) > README_LIMIT:
                    stderr.write(
                        f"[warning] {dir_name}/README.md truncated to {README_LIMIT} chars\n"
                    )
                    content = content[:README_LIMIT]
                return content
            except OSError:
                return ""
    return ""


def pull_ai(text: str, session: requests.Session) -> str:
    """Send text to the local LLM and return a 2-3 sentence summary, or '' on failure."""
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
                    + text
                ),
            },
        ],
    }
    try:
        response: requests.Response = session.post(
            LLM_URL,
            json=payload,
            timeout=LLM_TIMEOUT,
        )
        response.raise_for_status()
        resp_json: dict[str, Any] = response.json()
        return str(resp_json["choices"][0]["message"]["content"])
    except requests.exceptions.Timeout:
        stderr.write(f"\n[error] LLM request timed out after {LLM_TIMEOUT}s\n")
        return ""
    except requests.exceptions.RequestException as ex:
        stderr.write(f"\n[error] LLM network/HTTP error: {ex}\n")
        return ""
    except (KeyError, IndexError, ValueError) as ex:
        stderr.write(f"\n[error] Unexpected LLM response format: {ex}\n")
        return ""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Summarize README.md files in subdirectories using a local LLM."
    )
    parser.add_argument(
        "-d", "--directory", help="The base directory to scan", required=True
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output file for summaries (default: summaries.txt in the scanned directory)",
        default=None,
    )
    args = argparse.Namespace
    args = parser.parse_args()

    basedir: str = args.directory
    output_path: str = args.output or os.path.join(basedir, "summaries.txt")

    summaries: dict[str, str] = {}
    failed: list[str] = []

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

    if summaries:
        with open(output_path, "w", encoding="utf-8") as summ:
            for key in sorted(summaries):
                summ.write(f"{key}: {summaries[key]}\n\n")
        print(f"Summaries written to: {output_path}")
    else:
        print("No summaries to write.")

    if failed:
        print(f"Failed: {sorted(failed)}")
