#!/usr/bin/python3
"""Summarize the git repositories under a directory using a local LLM.

Every immediate subdirectory with a README is summarized from that README
plus its key build, dependency, CI and deployment files. The output file is a
self-contained Python script: a small viewer at the top, the summaries
embedded below it as JSON. Run the output file to browse them.

The previous output file doubles as the cache: a repo whose input files have
not changed since the last run keeps its summary instead of being sent to the
LLM again. Pass --force to redo everything.
"""

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import time
from datetime import UTC, datetime
from sys import stderr
from typing import Any

import requests

# LM Studio endpoint - adjust if your server runs elsewhere. The model check
# and the context-window lookup derive their URLs from this one.
LLM_URL: str = "http://127.0.0.1:1234/v1/chat/completions"
LLM_MODEL: str = "qwen/qwen3.6-35b-a3b"

# Max characters of project files sent to the LLM per repo. Lowered
# automatically when LM Studio reports a smaller loaded context window.
CONTEXT_CHARS: int = 65536

# Max tokens the model may generate per summary, -1 for no limit. The cap
# stops a looping model from running until the timeout kills it.
MAX_TOKENS: int = 8192

# Seconds to wait for the next chunk of a streamed reply, 0 to wait forever.
# Replies stream, so this is a stall limit, not a limit on the whole summary.
LLM_TIMEOUT: int = 300

# Attempts per repo before giving up on it for this run
RETRIES: int = 3

TEMPERATURE: float = 0.4

# HTTP statuses worth retrying. Anything else, such as a 404 for a bad model
# name, fails the repo at once.
RETRYABLE_HTTP: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})

# Rough characters per token, kept low so code-heavy input still fits
CHARS_PER_TOKEN: int = 3

# Bump this to invalidate every cached summary
CACHE_VERSION: int = 1

# Saved options live here unless -c/--config points somewhere else
CONFIG_NAME: str = "summarize_git.json"

# Built-in option values, used when neither the config nor the command line
# sets them. These keys are also the only ones read from or written to config.
DEFAULTS: dict[str, Any] = {
    "directory": None,
    "output": None,
    "url": LLM_URL,
    "model": LLM_MODEL,
    "timeout": LLM_TIMEOUT,
    "max_tokens": MAX_TOKENS,
    "context_chars": CONTEXT_CHARS,
    "retries": RETRIES,
    "quiet": False,
}

# README, README.md, Readme.rst, read_me.txt and friends, .md preferred
README_RE = re.compile(r"^read[-_]?me(\.[a-z0-9]+)?$", re.IGNORECASE)
README_EXT_PREF: dict[str, int] = {
    ".md": 0,
    ".markdown": 0,
    ".rst": 1,
    ".txt": 2,
    ".adoc": 2,
    "": 3,
}

# Character limits for the extra files sent alongside the main README
SECOND_README_CHARS: int = 4000
NESTED_README_CHARS: int = 2500
WORKFLOW_CHARS: int = 1500
MAX_WORKFLOWS: int = 3
MAX_NESTED_READMES: int = 6
TRUNC_MARK: str = "\n... [truncated] ..."

# Directories left out of directory listings and nested README searches
SKIP_DIRS: frozenset[str] = frozenset(
    {
        "node_modules",
        "vendor",
        "third_party_cache",
        "build",
        "dist",
        "out",
        "target",
        "__pycache__",
        "venv",
        "site-packages",
        "Pods",
        "DerivedData",
        "bazel-bin",
        "bazel-out",
        "bazel-testlogs",
        "cmake-build-debug",
        "cmake-build-release",
    }
)

# Hidden entries that are still worth showing in the directory listing
VISIBLE_HIDDEN: frozenset[str] = frozenset({".github", ".gitmodules", ".gitlab-ci.yml"})

# Files that tell the model about build, dependencies, usage and deployment,
# with a per-file character limit
CONTEXT_FILES: dict[str, int] = {
    # docs
    "INSTALL": 4000,
    "INSTALL.md": 4000,
    "BUILDING.md": 4000,
    "BUILD.md": 4000,
    "CONTRIBUTING.md": 2500,
    "USAGE.md": 4000,
    # metadata
    "catalog-info.yaml": 1500,
    "VERSION": 200,
    "CODEOWNERS": 800,
    # python
    "pyproject.toml": 3000,
    "setup.py": 2500,
    "setup.cfg": 2000,
    "requirements.txt": 2000,
    "requirements-dev.txt": 1000,
    "Pipfile": 1500,
    "environment.yml": 1500,
    # js / ts
    "package.json": 3000,
    # rust / go / swift / jvm / ruby / php
    "Cargo.toml": 2500,
    "go.mod": 2000,
    "Package.swift": 2000,
    "Podfile": 1500,
    "pom.xml": 3000,
    "build.gradle": 2500,
    "build.gradle.kts": 2500,
    "settings.gradle": 1000,
    "Gemfile": 1500,
    "composer.json": 2000,
    # c / c++ / general build
    "CMakeLists.txt": 3000,
    "Makefile": 2500,
    "makefile": 2500,
    "GNUmakefile": 2500,
    "meson.build": 2500,
    "configure.ac": 2000,
    "vcpkg.json": 1500,
    "conanfile.txt": 1500,
    "conanfile.py": 2000,
    "BUILD": 1500,
    "BUILD.bazel": 1500,
    "WORKSPACE": 1000,
    "MODULE.bazel": 1500,
    "Justfile": 1500,
    "justfile": 1500,
    "Taskfile.yml": 1500,
    "Taskfile.yaml": 1500,
    "build.sh": 2000,
    "install.sh": 1500,
    "release.sh": 1500,
    "entrypoint.sh": 1500,
    # CI
    ".gitlab-ci.yml": 2000,
    "Jenkinsfile": 1500,
    # containers / k8s / nix
    "Dockerfile": 2000,
    "Containerfile": 2000,
    "docker-compose.yml": 1500,
    "docker-compose.yaml": 1500,
    "compose.yml": 1500,
    "compose.yaml": 1500,
    "Chart.yaml": 1500,
    "values.yaml": 2500,
    "values.yml": 2500,
    "kustomization.yaml": 1500,
    "flake.nix": 2000,
    "default.nix": 1500,
    # repo structure
    ".gitmodules": 2000,
    # license (just the head, enough to identify it)
    "LICENSE": 400,
    "LICENSE.md": 400,
    "LICENSE.txt": 400,
    "COPYING": 400,
}
CONTEXT_SUFFIXES: dict[str, int] = {
    ".cabal": 2000,
    ".csproj": 2000,
    ".gemspec": 1500,
    ".podspec": 1500,
    ".nimble": 1500,
    ".opam": 1500,
}

SYSTEM_PROMPT: str = (
    "You are a technical writer summarizing software project documentation. "
    "You are given the README and key build, dependency, and deployment files "
    "of one project. You are accurate and never invent details that are not "
    "in the provided files."
)

# Filled with str.format(name=..., context=...). The context may contain
# braces; format leaves substituted values alone, so that is safe.
USER_PROMPT: str = (
    "Write a detailed summary of the project named below, based on the "
    "project files that follow.\n"
    "Cover these points, in this order, as flowing prose:\n"
    "1. What the project is and what problem it solves.\n"
    "2. Its main features and capabilities.\n"
    "3. The languages, frameworks, and notable dependencies it uses.\n"
    "4. How it is built, installed, configured, and run.\n"
    "5. Anything notable about its status, license, platform support, or "
    "known limitations.\n\n"
    "Aim for four to six paragraphs. If the files do not cover one of the "
    "points above, skip that point rather than guessing. Never invent "
    "commands, flags, versions, or dependencies. Use plain text only: no "
    "markdown, no headings, no bullet lists, no code fences. Do not include "
    "any preamble or closing remarks.\n\n"
    "Project name: {name}\n\n"
    "Project files follow. Each starts with a line of the form "
    "'----- FILE: name -----'.\n"
    "{context}"
)

# Some reasoning models emit a visible thinking block. Strip it if present.
THINK_RE = re.compile(r"<think>.*?</think>\s*", re.DOTALL | re.IGNORECASE)

# Finds the embedded JSON in an earlier output file, which serves as the cache
DATA_RE = re.compile(r'^_DATA = r"""\n(.*?)\n"""', re.MULTILINE | re.DOTALL)

VIEWER_PREFIX: str = '''#!/usr/bin/env python3
"""Repo summary browser.

Generated by summarize_git.py - regenerate rather than editing by hand.

Usage:
    ./summaries.py            interactive menu
    ./summaries.py NAME       print one summary and exit
    ./summaries.py --list     print the repo names and exit
    ./summaries.py --json     dump the raw JSON and exit
    ./summaries.py --text [PATH]
                              write every summary to a plain text file
                              (default: summaries.txt next to this script)
"""

import json
import os
import shutil
import sys
import textwrap

TEXT_WIDTH = 80

_DATA = r"""
'''

VIEWER_SUFFIX: str = '''"""


def load_data():
    try:
        return json.loads(_DATA)
    except ValueError as ex:
        sys.stderr.write("Embedded summary data is corrupt: %s\\n" % ex)
        raise SystemExit(1)


def term_width():
    width = shutil.get_terminal_size((80, 24)).columns - 2
    return max(40, min(width, 100))


def wrap(text, width=None):
    width = width or term_width()
    out = []
    for para in text.split("\\n"):
        para = para.strip()
        out.append(textwrap.fill(para, width=width) if para else "")
    return "\\n".join(out)


def details(entry):
    """Metadata lines shown under a repo's name."""
    git = entry.get("git") or {}
    lines = []
    if git.get("remote"):
        lines.append("Remote: %s" % git["remote"])
    if git.get("commit"):
        lines.append("Commit: %s @ %s" % (git.get("branch", "?"), git["commit"]))
    if entry.get("summarized"):
        lines.append("Summarized: %s" % entry["summarized"])
    return lines


def notes(entry):
    """Caveats shown after a summary."""
    out = []
    if entry.get("context_truncated"):
        out.append("(Summary based on truncated input.)")
    if entry.get("summary_truncated"):
        out.append("(Summary cut off at the model's token limit.)")
    return out


def show(name, entry):
    width = term_width()
    print()
    print("=" * width)
    print(name)
    print("=" * width)
    for line in details(entry):
        print(line)
    print()
    print(wrap(entry.get("summary", "(no summary)")))
    for note in notes(entry):
        print()
        print(note)
    print()


def write_text(path, names, repos, meta):
    rule = "=" * TEXT_WIDTH
    lines = [
        "Repo summaries (%d)" % len(names),
        "Generated %s by %s" % (meta.get("generated", "?"), meta.get("model", "?")),
    ]
    if meta.get("source_directory"):
        lines.append("Source: %s" % meta["source_directory"])
    lines.append("")
    for name in names:
        entry = repos[name]
        lines.extend([rule, name, rule])
        lines.extend(details(entry))
        lines.append("")
        lines.append(wrap(entry.get("summary", "(no summary)"), TEXT_WIDTH))
        for note in notes(entry):
            lines.extend(["", note])
        lines.append("")
    try:
        with open(path, "w", encoding="utf-8") as out:
            out.write("\\n".join(lines))
    except OSError as ex:
        sys.stderr.write("Could not write %s: %s\\n" % (path, ex))
        return 1
    print("Wrote %d summaries to: %s" % (len(names), path))
    return 0


def print_menu(names, meta):
    width = term_width()
    print()
    print("=" * width)
    print("Repo summaries (%d) - generated %s by %s"
          % (len(names), meta.get("generated", "?"), meta.get("model", "?")))
    print("=" * width)
    pad = len(str(len(names)))
    for index, name in enumerate(names, start=1):
        print("%*d. %s" % (pad, index, name))
    print()


def menu(names, repos, meta):
    while True:
        print_menu(names, meta)
        try:
            choice = input("Choose a repo by number or name (q to quit): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return

        if choice.lower() in ("q", "quit", "exit"):
            return
        if not choice:
            continue

        name = resolve(choice, names)
        if name is None:
            print("\\nNo such repo: %s" % choice)
            continue

        show(name, repos[name])
        try:
            input("Press Enter to return to the menu... ")
        except (EOFError, KeyboardInterrupt):
            print()
            return


def resolve(choice, names):
    if choice.isdigit():
        index = int(choice)
        if 1 <= index <= len(names):
            return names[index - 1]
        return None
    lowered = choice.lower()
    for name in names:
        if name.lower() == lowered:
            return name
    matches = [name for name in names if name.lower().startswith(lowered)]
    if len(matches) == 1:
        return matches[0]
    return None


def main(argv):
    data = load_data()
    repos = data.get("repos", {})
    names = sorted(repos)
    meta = {
        "generated": data.get("generated", "?"),
        "model": data.get("model", "?"),
        "source_directory": data.get("source_directory"),
    }

    if not names:
        print("No summaries in this file.")
        return 0

    if argv:
        arg = argv[0]
        if arg in ("-h", "--help"):
            print(__doc__.strip())
            return 0
        if arg in ("-j", "--json"):
            print(json.dumps(data, indent=2))
            return 0
        if arg in ("-l", "--list"):
            for name in names:
                print(name)
            return 0
        if arg in ("-t", "--text") or arg.startswith("--text="):
            if arg.startswith("--text="):
                path = arg.split("=", 1)[1]
            elif len(argv) > 1:
                path = argv[1]
            else:
                path = ""
            if not path:
                here = os.path.dirname(os.path.abspath(__file__))
                path = os.path.join(here, "summaries.txt")
            return write_text(path, names, repos, meta)
        name = resolve(arg, names)
        if name is None:
            sys.stderr.write("No such repo: %s\\n" % arg)
            return 1
        show(name, repos[name])
        return 0

    menu(names, repos, meta)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
'''


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def get_dirs(base_dir: str = ".") -> list[str]:
    """Return a sorted list of non-hidden subdirectory names in base_dir."""
    dirs: list[str] = []
    with os.scandir(base_dir) as it:
        for entry in it:
            if not entry.name.startswith(".") and entry.is_dir():
                dirs.append(entry.name)
    return sorted(dirs)


# --------------------------------------------------------------------------- #
# Context gathering
# --------------------------------------------------------------------------- #


def find_readmes(filenames: list[str]) -> list[str]:
    """Return the README-like names in filenames, best candidate first."""

    def key(name: str) -> tuple[int, str]:
        ext: str = os.path.splitext(name)[1].lower()
        return (README_EXT_PREF.get(ext, 4), name.lower())

    return sorted((f for f in filenames if README_RE.match(f)), key=key)


def read_text(path: str, limit: int) -> str:
    """Return up to limit characters of the file at path, or '' if unreadable."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            data: str = f.read(limit + 1)
    except OSError:
        return ""
    if len(data) > limit:
        return data[:limit] + TRUNC_MARK
    return data


def context_limit(fname: str) -> int | None:
    """Return the character limit for a build/deploy file, or None if not one."""
    lim: int | None = CONTEXT_FILES.get(fname)
    if lim is None:
        lim = CONTEXT_SUFFIXES.get(os.path.splitext(fname)[1])
    if lim is None and fname.startswith(("Dockerfile.", "Containerfile.")):
        lim = 1500
    if lim is None and fname.startswith("entrypoint") and fname.endswith(".sh"):
        lim = 1500
    return lim


def dir_listing(path: str, max_lines: int = 80) -> str:
    """Two-level listing of path, skipping hidden and build-output directories."""
    try:
        with os.scandir(path) as it:
            entries = sorted(it, key=lambda e: (not e.is_dir(), e.name.lower()))
    except OSError:
        return ""
    lines: list[str] = []
    for entry in entries:
        if entry.name.startswith(".") and entry.name not in VISIBLE_HIDDEN:
            continue
        if entry.is_dir():
            if entry.name in SKIP_DIRS:
                continue
            lines.append(entry.name + "/")
            try:
                with os.scandir(entry.path) as it:
                    kids = sorted(
                        (k for k in it if not k.name.startswith(".")),
                        key=lambda k: k.name.lower(),
                    )
            except OSError:
                kids = []
            for kid in kids[:8]:
                lines.append("    " + kid.name + ("/" if kid.is_dir() else ""))
            if len(kids) > 8:
                lines.append("    ...")
        else:
            lines.append(entry.name)
        if len(lines) >= max_lines:
            lines.append("... [listing truncated]")
            break
    return "\n".join(lines)


def nested_readmes(root: str, max_depth: int = 2) -> list[str]:
    """Return paths of READMEs in subdirectories of root, skipping nested repos."""
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        rel: str = os.path.relpath(dirpath, root)
        depth: int = 0 if rel == "." else rel.count(os.sep) + 1
        if dirpath != root and (".git" in dirnames or ".git" in filenames):
            dirnames[:] = []  # a submodule or vendored repo: a different project
            continue
        if depth >= max_depth:
            dirnames[:] = []
        else:
            dirnames[:] = sorted(
                d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")
            )
        if dirpath == root:
            continue
        readmes: list[str] = find_readmes(filenames)
        if readmes:
            found.append(os.path.join(dirpath, readmes[0]))
            if len(found) >= MAX_NESTED_READMES:
                break
    return found


def gather_context(repo_dir: str, budget: int) -> tuple[str, bool, str]:
    """Collect the README and key project files of one repo, within budget chars.

    Returns (context, truncated, main_readme). context is '' when the repo has
    no README at all, in which case it is skipped.
    """
    try:
        files: list[str] = os.listdir(repo_dir)
    except OSError:
        return "", False, ""

    readmes: list[str] = find_readmes(files)
    docs: str = os.path.join(repo_dir, "docs")
    if not readmes and os.path.isdir(docs):
        try:
            readmes = ["docs/" + f for f in find_readmes(os.listdir(docs))]
        except OSError:
            readmes = []
    if not readmes:
        return "", False, ""

    sections: list[tuple[str, str]] = []  # (title, text) in priority order

    # 1. The main README gets half the budget; a second one a fixed slice.
    for index, name in enumerate(readmes[:2]):
        limit: int = budget // 2 if index == 0 else SECOND_README_CHARS
        sections.append((name, read_text(os.path.join(repo_dir, name), limit)))

    # 2. Build, dependency and deployment files
    for fname in sorted(files):
        lim: int | None = context_limit(fname)
        full: str = os.path.join(repo_dir, fname)
        if lim and os.path.isfile(full):
            sections.append((fname, read_text(full, lim)))

    # 3. CI workflows, often the most accurate build instructions
    workflows: str = os.path.join(repo_dir, ".github", "workflows")
    if os.path.isdir(workflows):
        try:
            names: list[str] = sorted(
                f for f in os.listdir(workflows) if f.endswith((".yml", ".yaml"))
            )
        except OSError:
            names = []
        for fname in names[:MAX_WORKFLOWS]:
            sections.append(
                (
                    ".github/workflows/" + fname,
                    read_text(os.path.join(workflows, fname), WORKFLOW_CHARS),
                )
            )

    # 4. Directory layout
    sections.append(("Directory layout (top two levels)", dir_listing(repo_dir)))

    # 5. READMEs of sub-components
    for nested in nested_readmes(repo_dir):
        sections.append(
            (os.path.relpath(nested, repo_dir), read_text(nested, NESTED_README_CHARS))
        )

    # Assemble within the budget. Per-file limits on the extra files are
    # deliberate slices, so only a cut main README or a budget overflow counts
    # as truncated input.
    out: list[str] = []
    used: int = 0
    truncated: bool = sections[0][1].endswith(TRUNC_MARK)
    for title, text in sections:
        if not text.strip():
            continue
        block: str = f"\n----- FILE: {title} -----\n{text}\n"
        if used + len(block) > budget:
            truncated = True
            remaining: int = budget - used
            if remaining > 500:
                out.append(block[:remaining] + "\n... [truncated: input budget] ...\n")
            break
        out.append(block)
        used += len(block)
    return "".join(out), truncated, readmes[0]


# --------------------------------------------------------------------------- #
# Git metadata
# --------------------------------------------------------------------------- #


def run_git(path: str, *args: str) -> str:
    """Return stdout of a git command run in path, or '' on any failure."""
    try:
        result = subprocess.run(
            ["git", "-C", path, *args],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def scrub_url(url: str) -> str:
    """Remove embedded credentials: https://user:token@host/x -> https://host/x"""
    return re.sub(r"^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@]+@", r"\1", url)


def git_info(path: str) -> dict[str, str]:
    """Return remote, branch and last commit of the repo at path, if it is one."""
    if not os.path.exists(os.path.join(path, ".git")):
        return {}
    info: dict[str, str] = {
        "remote": scrub_url(run_git(path, "config", "--get", "remote.origin.url")),
        "branch": run_git(path, "rev-parse", "--abbrev-ref", "HEAD"),
        "commit": run_git(path, "log", "-1", "--format=%h (%cs) %s"),
    }
    return {key: value for key, value in info.items() if value}


# --------------------------------------------------------------------------- #
# LLM server
# --------------------------------------------------------------------------- #


def server_urls(url: str) -> tuple[str, str] | None:
    """Return the (/v1/models, /api/v0/models) URLs that go with a chat URL."""
    suffix: str = "/v1/chat/completions"
    if not url.endswith(suffix):
        return None
    base: str = url[: -len(suffix)]
    return base + "/v1/models", base + "/api/v0/models"


def check_model(session: requests.Session, url: str, model: str) -> str | None:
    """Verify the server is up and offers the model. Return an error or None."""
    urls = server_urls(url)
    if urls is None:
        stderr.write(
            "[warning] URL does not end in /v1/chat/completions; "
            "skipping the model check\n"
        )
        return None
    try:
        response: requests.Response = session.get(urls[0], timeout=30)
        response.raise_for_status()
        data: Any = response.json()
        ids: list[str] = [str(m.get("id")) for m in data.get("data", [])]
    except requests.exceptions.RequestException as ex:
        return (
            f"cannot reach the LLM server at {urls[0]}: {ex}\n"
            "Is LM Studio running with its server started?"
        )
    except (ValueError, AttributeError, TypeError) as ex:
        return f"unexpected response from {urls[0]}: {ex}"
    if model not in ids:
        available: str = "\n    ".join(sorted(ids)) or "(none)"
        return f"model '{model}' is not available at {urls[0]}.\n  Available:\n    {available}"
    return None


def model_status(session: requests.Session, url: str, model: str) -> tuple[str, int]:
    """Return (state, loaded context tokens) from LM Studio's native API.

    Returns ('', 0) when the server is not LM Studio or says nothing useful.
    """
    urls = server_urls(url)
    if urls is None:
        return "", 0
    try:
        response: requests.Response = session.get(urls[1], timeout=30)
        response.raise_for_status()
        data: Any = response.json()
        models: list[Any] = data.get("data", []) if isinstance(data, dict) else []
    except (requests.exceptions.RequestException, ValueError):
        return "", 0
    for entry in models:
        if isinstance(entry, dict) and entry.get("id") == model:
            state: str = str(entry.get("state") or "")
            tokens: Any = entry.get("loaded_context_length") if state == "loaded" else 0
            return state, int(tokens or 0)
    return "", 0


def fit_budget(context_tokens: int, max_tokens: int) -> int:
    """Chars of input that fit a context window beside the prompt and the reply."""
    reserve: int = max_tokens if max_tokens > 0 else 2048
    reserve = min(reserve, context_tokens // 2)
    return max(4000, (context_tokens - 1000 - reserve) * CHARS_PER_TOKEN)


class LLMHTTPError(Exception):
    """An HTTP error from the LLM server, with the body read before the stream closed."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"HTTP {status}: {body}")
        self.status: int = status
        self.body: str = body


def stream_chat(
    session: requests.Session, url: str, payload: dict[str, Any], timeout: int | None
) -> tuple[str, str | None]:
    """Run one streaming chat request. Return (content, finish_reason)."""
    parts: list[str] = []
    finish: str | None = None
    chunks: int = 0
    with session.post(url, json=payload, stream=True, timeout=timeout) as response:
        if not response.ok:
            raise LLMHTTPError(response.status_code, response.text[:300].strip())
        for raw in response.iter_lines():
            line: str = raw.decode("utf-8", errors="replace")
            if not line.startswith("data:"):
                continue
            data: str = line[5:].strip()
            if data == "[DONE]":
                break
            chunk: Any = json.loads(data)
            if chunk.get("error"):
                raise RuntimeError(f"LLM error: {chunk['error']}")
            choices: list[Any] = chunk.get("choices") or []
            if not choices:
                continue
            delta: dict[str, Any] = choices[0].get("delta") or {}
            parts.append(delta.get("content") or "")
            if choices[0].get("finish_reason"):
                finish = str(choices[0]["finish_reason"])
            chunks += 1
            if chunks % 50 == 0:
                print(".", end="", flush=True)
    return THINK_RE.sub("", "".join(parts)).strip(), finish


def pull_ai(
    session: requests.Session, options: dict[str, Any], name: str, context: str
) -> tuple[str, bool]:
    """Summarize one repo. Return (summary, cut_off); raise RuntimeError on failure."""
    payload: dict[str, Any] = {
        "model": options["model"],
        "temperature": TEMPERATURE,
        "max_tokens": options["max_tokens"],
        "stream": True,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": USER_PROMPT.format(name=name, context=context),
            },
        ],
    }
    timeout: int | None = options["timeout"] or None
    retries: int = options["retries"]
    last_err: str = "no attempts made"
    for attempt in range(1, retries + 1):
        try:
            content, finish = stream_chat(session, options["url"], payload, timeout)
        except LLMHTTPError as ex:
            last_err = str(ex)
            if ex.status not in RETRYABLE_HTTP:
                break
        except requests.exceptions.Timeout:
            last_err = f"no data from the LLM for {timeout}s"
        except (requests.exceptions.RequestException, ValueError, RuntimeError) as ex:
            last_err = str(ex) or type(ex).__name__
        else:
            if content:
                return content, finish == "length"
            last_err = "empty response"
        stderr.write(f"\n[warning] attempt {attempt} failed: {last_err}\n")
        if attempt < retries:
            time.sleep(2 * attempt)
    raise RuntimeError(last_err)


# --------------------------------------------------------------------------- #
# Output and cache
# --------------------------------------------------------------------------- #


def render_output(data: dict[str, Any]) -> str:
    """Return the full text of the self-contained viewer script."""
    # ensure_ascii keeps the blob pure ASCII, and JSON escaping guarantees it
    # can never contain a triple quote, so the raw string below is always safe.
    blob: str = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=True)
    if '"""' in blob:
        raise ValueError("summary data contains a triple quote, cannot embed")
    return VIEWER_PREFIX + blob + "\n" + VIEWER_SUFFIX


def atomic_write(path: str, text: str) -> None:
    """Write via a temp file and rename so an interrupted write never corrupts path."""
    tmp: str = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def make_executable(path: str) -> None:
    try:
        mode = os.stat(path).st_mode
        os.chmod(path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    except OSError as ex:
        stderr.write(f"[warning] could not mark {path} executable: {ex}\n")


def write_output(path: str, data: dict[str, Any]) -> None:
    atomic_write(path, render_output(data))
    make_executable(path)


def load_previous(path: str) -> dict[str, dict[str, Any]]:
    """Return the repo entries embedded in an earlier output file, or {}."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            text: str = f.read()
    except FileNotFoundError:
        return {}
    except OSError as ex:
        stderr.write(f"[warning] ignoring unreadable {path}: {ex}\n")
        return {}
    match = DATA_RE.search(text)
    if match is None:
        stderr.write(f"[warning] {path} is not a summaries file; starting fresh\n")
        return {}
    try:
        data: Any = json.loads(match.group(1))
    except ValueError as ex:
        stderr.write(f"[warning] {path} holds corrupt data ({ex}); starting fresh\n")
        return {}
    repos: Any = data.get("repos") if isinstance(data, dict) else None
    return repos if isinstance(repos, dict) else {}


def cache_key(model: str, name: str, context: str) -> str:
    """Hash of everything that shapes a summary, so any change redoes it."""
    blob: str = json.dumps(
        {
            "v": CACHE_VERSION,
            "model": model,
            "system": SYSTEM_PROMPT,
            "prompt": USER_PROMPT,
            "temperature": TEMPERATURE,
            "name": name,
            "context": context,
        },
        sort_keys=True,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def is_fresh(entry: dict[str, Any] | None, key: str) -> bool:
    """True when a cached entry can stand in for a new summary."""
    return bool(
        entry
        and entry.get("hash") == key
        and entry.get("summary")
        and not entry.get("summary_truncated")
    )


def cache_status(entry: dict[str, Any] | None, key: str, force: bool) -> str:
    """Why a repo will or will not be summarized: cached, retry, changed, new or forced."""
    if force:
        return "forced" if entry else "new"
    if is_fresh(entry, key):
        return "cached"
    if entry and entry.get("hash") == key:
        return "retry"  # the last summary was cut off
    return "changed" if entry else "new"


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


def default_config_path() -> str:
    """Return $XDG_CONFIG_HOME/summarize_git.json, falling back to ~/.config."""
    base: str = os.environ.get("XDG_CONFIG_HOME") or os.path.join(
        os.path.expanduser("~"), ".config"
    )
    return os.path.join(base, CONFIG_NAME)


def load_config(path: str) -> dict[str, Any]:
    """Return the known options stored in the JSON config at path, or {}."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw: Any = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as ex:
        stderr.write(f"[warning] ignoring unreadable config {path}: {ex}\n")
        return {}
    if not isinstance(raw, dict):
        stderr.write(f"[warning] ignoring config {path}: not a JSON object\n")
        return {}
    unknown: list[str] = sorted(set(raw) - set(DEFAULTS))
    if unknown:
        stderr.write(f"[warning] unknown keys in {path}: {unknown}\n")
    return {key: raw[key] for key in DEFAULTS if key in raw}


def save_config(path: str, options: dict[str, Any]) -> None:
    """Write options to the JSON config at path, creating its directory."""
    saved: dict[str, Any] = dict(options)
    # Store absolute paths so the config works from any working directory.
    for key in ("directory", "output"):
        if saved.get(key):
            saved[key] = os.path.abspath(os.path.expanduser(saved[key]))
    parent: str = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(saved, f, indent=2, sort_keys=True)
        f.write("\n")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize the git repositories under a directory using a local LLM.",
        epilog=(
            "Options are read from the config file first; anything given on "
            "the command line overrides it. Use --save to store the "
            "current options for next time. The previous output file is the "
            "cache: repos whose files have not changed are not re-summarized."
        ),
    )
    # Option defaults are None so we can tell what was actually given and
    # layer: command line > config file > built-in DEFAULTS.
    parser.add_argument(
        "-c",
        "--config",
        help=f"Config file to use (default: {default_config_path()})",
        default=default_config_path(),
    )
    parser.add_argument(
        "-s",
        "--save",
        action="store_true",
        help="Save the effective options to the config file",
    )
    parser.add_argument("-d", "--directory", help="The base directory to scan")
    parser.add_argument(
        "-o",
        "--output",
        help="Output script (default: summaries.py in the scanned directory)",
    )
    parser.add_argument("-u", "--url", help=f"LLM endpoint URL (default: {LLM_URL})")
    parser.add_argument("-m", "--model", help=f"LLM model name (default: {LLM_MODEL})")
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        help=(
            "Seconds to wait for the next chunk of a reply, 0 for no limit "
            f"(default: {LLM_TIMEOUT})"
        ),
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        help=f"Max tokens per summary, -1 for no limit (default: {MAX_TOKENS})",
    )
    parser.add_argument(
        "--context-chars",
        type=int,
        help=(
            "Max characters of project files sent per repo; lowered automatically "
            f"to fit the loaded model's context window (default: {CONTEXT_CHARS})"
        ),
    )
    parser.add_argument(
        "--retries",
        type=int,
        help=f"Attempts per repo before giving up on it (default: {RETRIES})",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action=argparse.BooleanOptionalAction,
        help="Do not echo each summary while working",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Ignore cached summaries and redo every repo",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="List what would be summarized, and what is cached, then exit",
    )
    args = parser.parse_args()

    options: dict[str, Any] = dict(DEFAULTS)
    options.update(load_config(args.config))
    for key in DEFAULTS:
        value: Any = getattr(args, key)
        if value is not None:
            options[key] = value

    if args.save:
        try:
            save_config(args.config, options)
        except OSError as ex:
            stderr.write(f"[error] could not save config {args.config}: {ex}\n")
            return 1
        print(f"Saved options to: {args.config}")

    if not options["directory"]:
        if args.save:
            return 0
        parser.error(
            "no directory to scan: pass -d/--directory "
            f"(add --save to remember it in {args.config})"
        )

    if options["retries"] < 1:
        parser.error("--retries must be at least 1")
    if options["timeout"] < 0:
        parser.error("--timeout must be 0 or more")
    if options["context_chars"] < 4000:
        parser.error("--context-chars must be at least 4000")
    if options["max_tokens"] != -1 and options["max_tokens"] < 1:
        parser.error("--max-tokens must be -1 or a positive number")

    basedir: str = os.path.expanduser(options["directory"])
    output_path: str = os.path.expanduser(
        options["output"] or os.path.join(basedir, "summaries.py")
    )
    if not os.path.isdir(basedir):
        stderr.write(f"[error] not a directory: {basedir}\n")
        return 1

    previous: dict[str, dict[str, Any]] = load_previous(output_path)
    budget: int = options["context_chars"]
    repos: dict[str, dict[str, Any]] = {}
    failed: list[str] = []

    def write() -> None:
        write_output(
            output_path,
            {
                "generated": utc_now(),
                "model": options["model"],
                "source_directory": os.path.abspath(basedir),
                "repos": repos,
            },
        )

    try:
        with requests.Session() as session:
            if not args.dry_run:
                error = check_model(session, options["url"], options["model"])
                if error:
                    stderr.write(f"[error] {error}\n")
                    return 1
            # Fit the input to the loaded model's context window. A dry run does
            # this too (when the server is up) so its cache verdicts match.
            state, context_tokens = model_status(
                session, options["url"], options["model"]
            )
            if context_tokens:
                fit: int = fit_budget(context_tokens, options["max_tokens"])
                if fit < budget:
                    print(
                        f"[info] model context is {context_tokens:,} tokens; "
                        f"limiting input to {fit:,} chars per repo"
                    )
                    budget = fit
            elif state and state != "loaded" and not args.dry_run:
                print(
                    "[info] model is not loaded yet; LM Studio will load it "
                    "on the first request"
                )

            # Discovery: every subdirectory with a README, with its context ready
            work: list[tuple[str, str, bool, str]] = []
            for subdir in get_dirs(basedir):
                context, truncated, readme = gather_context(
                    os.path.join(basedir, subdir), budget
                )
                if context:
                    work.append((subdir, context, truncated, readme))
            if not work:
                print(f"No READMEs found under {basedir}.")
                return 0

            keys: dict[str, str] = {
                subdir: cache_key(options["model"], subdir, context)
                for subdir, context, _, _ in work
            }
            # Keep the previous entries of repos still present, so an interrupted
            # run writes a file that loses nothing.
            repos.update(
                {
                    subdir: previous[subdir]
                    for subdir, _, _, _ in work
                    if subdir in previous
                }
            )

            if args.dry_run:
                print(f"Found {len(work)} repos with a README under {basedir}")
                cached: int = 0
                for subdir, context, truncated, readme in work:
                    status: str = cache_status(
                        previous.get(subdir), keys[subdir], args.force
                    )
                    fresh: bool = status == "cached"
                    cached += fresh
                    extra: str = ", input truncated" if truncated else ""
                    print(
                        f"  {status:8} {subdir}  ({readme}, {len(context):,} chars{extra})"
                    )
                print(f"{cached} cached, {len(work) - cached} to summarize")
                return 0

            for index, (subdir, context, truncated, readme) in enumerate(work, 1):
                tag: str = f"[{index}/{len(work)}] {subdir}"
                info: dict[str, str] = git_info(os.path.join(basedir, subdir))
                if not args.force and is_fresh(previous.get(subdir), keys[subdir]):
                    entry: dict[str, Any] = dict(previous[subdir])
                    entry.pop("git", None)
                    if info:
                        entry["git"] = info
                    repos[subdir] = entry
                    print(f"{tag}: cached")
                    continue

                print(f"{tag}: summarizing {len(context):,} chars ", end="", flush=True)
                started: float = time.monotonic()
                try:
                    summary, cut = pull_ai(session, options, subdir, context)
                except RuntimeError as ex:
                    failed.append(subdir)
                    stderr.write(f"\n[error] {subdir}: {ex}\n")
                    continue
                print(f"done in {time.monotonic() - started:.0f}s", flush=True)
                if cut:
                    stderr.write(
                        f"[warning] {subdir}: summary cut off at "
                        f"{options['max_tokens']} tokens; it will be retried next "
                        "run unless --max-tokens is raised\n"
                    )
                entry = {
                    "summary": summary,
                    "hash": keys[subdir],
                    "summarized": utc_now(),
                    "readme": readme,
                    "context_chars": len(context),
                    "context_truncated": truncated,
                    "summary_truncated": cut,
                }
                if info:
                    entry["git"] = info
                repos[subdir] = entry
                if not options["quiet"]:
                    print(f"{summary}\n")
                write()
    except KeyboardInterrupt:
        stderr.write("\n[interrupted] saving what finished so far\n")
        if repos:
            write()
        return 130

    if repos:
        write()
        print(f"Wrote {len(repos)} summaries to: {output_path}")
        run_path: str = (
            output_path if os.path.isabs(output_path) else "./" + output_path
        )
        print(f"Run it with: {run_path}")
        print(f"Write a text copy with: {run_path} --text")
    else:
        print("No summaries to write.")

    if failed:
        print(f"Failed: {sorted(failed)} (re-run to retry them)")

    return 1 if failed and not repos else 0


if __name__ == "__main__":
    raise SystemExit(main())
