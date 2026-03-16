#!/usr/bin/python3
import argparse
import json
import os
from sys import stderr
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

# LM Studio endpoint — adjust if your server runs elsewhere
LLM_BASE_URL: str = "http://127.0.0.1:1234"
LLM_URL: str = f"{LLM_BASE_URL}/v1/chat/completions"
LLM_MODELS_URL: str = f"{LLM_BASE_URL}/v1/models"
LLM_MODEL: str = "openai/gpt-oss-20b"

# Fallback max characters of README content if context size cannot be determined
README_LIMIT_DEFAULT: int = 10240

# Fraction of context to reserve for prompt overhead + response tokens (25%)
CONTEXT_RESERVE: float = 0.25

# Approximate characters per token — good enough for English prose
CHARS_PER_TOKEN: float = 4.0

# Seconds to wait for any LLM request before giving up
LLM_TIMEOUT: int = 60


def get_dirs(base_dir: str = ".") -> list[str]:
    """Return a sorted list of non-hidden subdirectory names in base_dir."""
    dirs: list[str] = []
    with os.scandir(base_dir) as it:
        for entry in it:
            if not entry.name.startswith(".") and entry.is_dir():
                dirs.append(entry.name)
    return sorted(dirs)


def pull_readme(base_dir: str, dir_name: str, readme_limit: int) -> str:
    """Return the (possibly truncated) contents of README.md in base_dir/dir_name, or '' if absent."""
    full_dir: str = os.path.join(base_dir, dir_name)
    try:
        with os.scandir(full_dir) as it:
            for entry in it:
                if entry.name.lower() == "readme.md" and entry.is_file():
                    try:
                        with open(
                            entry.path, "r", encoding="utf-8", errors="replace"
                        ) as f:
                            content: str = f.read()
                        if len(content) > readme_limit:
                            stderr.write(
                                f"[warning] {dir_name}/README.md truncated to {readme_limit} chars\n"
                            )
                            content = content[:readme_limit]
                        return content
                    except OSError:
                        return ""
    except OSError:
        return ""
    return ""


def fetch_context_limit() -> int:
    """Query the LM Studio /v1/models endpoint and derive a safe README char limit.
    Returns README_LIMIT_DEFAULT on any failure."""
    req = Request(LLM_MODELS_URL, method="GET")
    try:
        with urlopen(req, timeout=LLM_TIMEOUT) as response:
            if response.status != 200:
                raise ValueError(f"HTTP {response.status}")
            data: dict[str, Any] = json.loads(response.read().decode("utf-8"))

            # Find our model in the list
            models: list[dict] = data.get("data", [])
            model_info: dict | None = next(
                (m for m in models if m.get("id") == LLM_MODEL), None
            )
            if model_info is None:
                stderr.write(
                    f"[warning] Model '{LLM_MODEL}' not found in /v1/models response — "
                    f"using default README limit of {README_LIMIT_DEFAULT} chars\n"
                )
                return README_LIMIT_DEFAULT

            context_length: int | None = (
                model_info.get("context_length")
                or model_info.get("max_context_length")
                or (model_info.get("meta") or {}).get("context_length")
            )
            if not context_length:
                stderr.write(
                    f"[warning] Could not determine context length for '{LLM_MODEL}' — "
                    f"using default README limit of {README_LIMIT_DEFAULT} chars\n"
                )
                return README_LIMIT_DEFAULT

            usable_tokens: float = context_length * (1.0 - CONTEXT_RESERVE)
            readme_limit: int = int(usable_tokens * CHARS_PER_TOKEN)
            print(
                f"[info] Model context: {context_length} tokens → "
                f"README limit set to {readme_limit} chars "
                f"({100 * (1 - CONTEXT_RESERVE):.0f}% usable)\n"
            )
            return readme_limit

    except TimeoutError:
        stderr.write(
            f"[warning] Timed out querying model context size — "
            f"using default README limit of {README_LIMIT_DEFAULT} chars\n"
        )
    except (URLError, ValueError, KeyError) as ex:
        stderr.write(
            f"[warning] Could not query model context size ({ex}) — "
            f"using default README limit of {README_LIMIT_DEFAULT} chars\n"
        )
    return README_LIMIT_DEFAULT


def pull_ai(text: str) -> str:
    """Send text to the local LLM and return a 2-3 sentence summary, or '' on failure."""
    if not text:
        return ""

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
                    "Please write a summary of the following document. No more than 1 paragraph.\n\n"
                    + text
                ),
            },
        ],
    }

    body: bytes = json.dumps(payload).encode("utf-8")
    req = Request(
        LLM_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(req, timeout=LLM_TIMEOUT) as response:
            if response.status != 200:
                stderr.write(f"\n[error] LLM returned HTTP {response.status}\n")
                return ""
            resp_json: dict[str, Any] = json.loads(response.read().decode("utf-8"))
            return str(resp_json["choices"][0]["message"]["content"])
    except TimeoutError:
        stderr.write(f"\n[error] LLM request timed out after {LLM_TIMEOUT}s\n")
        return ""
    except URLError as ex:
        stderr.write(f"\n[error] LLM network/HTTP error: {ex.reason}\n")
        if hasattr(ex, "read"):
            stderr.write(
                f"[error] Response body: {ex.read().decode('utf-8', errors='replace')}\n"
            )
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
    args = parser.parse_args()

    basedir: str = args.directory
    output_path: str = args.output or os.path.join(basedir, "summaries.txt")

    # Determine README limit from model context size before processing
    readme_limit: int = fetch_context_limit()

    summaries: dict[str, str] = {}
    failed: list[str] = []

    for subdir in get_dirs(basedir):
        text: str = pull_readme(basedir, subdir, readme_limit)
        if not text:
            continue
        summary: str = pull_ai(text)
        if summary:
            summaries[subdir] = summary
            print(f"{subdir}: {summary}\n")
        else:
            failed.append(subdir)

    if summaries:
        with open(output_path, "w", encoding="utf-8") as summ:
            summ.writelines(f"{key}: {summaries[key]}\n\n" for key in sorted(summaries))
        print(f"Summaries written to: {output_path}")
    else:
        print("No summaries to write.")

    if failed:
        print(f"Failed: {sorted(failed)}")
