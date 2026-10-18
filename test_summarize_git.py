"""Tests for the parts of summarize_git.py that need no LLM server."""

import importlib
import io
import json
import os
import shutil
import subprocess
import sys

import pytest


@pytest.fixture(scope="module")
def sg():
    pytest.importorskip("requests")
    return importlib.import_module("summarize_git")


class FakeResponse:
    """Stands in for a streamed requests.Response."""

    def __init__(self, status, lines=(), text=""):
        self.status_code = status
        self.ok = status < 400
        self.text = text
        self._lines = lines

    def iter_lines(self):
        yield from (line.encode() for line in self._lines)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, json=None, stream=False, timeout=None):
        self.calls.append(json)
        return self.responses.pop(0)


def sse(*pieces, finish="stop"):
    lines = []
    for piece in pieces:
        chunk = {"choices": [{"delta": {"content": piece}, "finish_reason": None}]}
        lines += ["data: " + json.dumps(chunk), ""]
    lines += [
        "data: " + json.dumps({"choices": [{"delta": {}, "finish_reason": finish}]}),
        "",
        "data: [DONE]",
    ]
    return lines


def test_find_readmes_prefers_markdown_and_ignores_lookalikes(sg):
    names = [
        "readme.txt",
        "README",
        "README.md",
        "Readme.rst",
        "notes.md",
        "README.en.md",
    ]
    assert sg.find_readmes(names) == ["README.md", "Readme.rst", "readme.txt", "README"]


def test_scrub_url_removes_credentials_only(sg):
    assert (
        sg.scrub_url("https://u:tok@github.com/a/b.git") == "https://github.com/a/b.git"
    )
    assert sg.scrub_url("git@github.com:a/b.git") == "git@github.com:a/b.git"
    assert sg.scrub_url("https://github.com/a/b.git") == "https://github.com/a/b.git"


def test_fit_budget(sg):
    assert sg.fit_budget(8192, 8192) == (8192 - 1000 - 4096) * sg.CHARS_PER_TOKEN
    assert sg.fit_budget(4096, 8192) == 4000  # never below the floor
    assert sg.fit_budget(32768, -1) == (32768 - 1000 - 2048) * sg.CHARS_PER_TOKEN


def test_server_urls(sg):
    chat = "http://host:1234/v1/chat/completions"
    assert sg.server_urls(chat) == (
        "http://host:1234/v1/models",
        "http://host:1234/api/v0/models",
    )
    assert sg.server_urls("http://host:1234/other") is None


def test_cache_status(sg):
    good = {"hash": "k", "summary": "text", "summary_truncated": False}
    cut = {"hash": "k", "summary": "text", "summary_truncated": True}
    assert sg.cache_status(None, "k", False) == "new"
    assert sg.cache_status(good, "k", False) == "cached"
    assert sg.cache_status(good, "other", False) == "changed"
    assert sg.cache_status(cut, "k", False) == "retry"
    assert sg.cache_status(good, "k", True) == "forced"
    assert sg.is_fresh(good, "k") and not sg.is_fresh(cut, "k")


def test_output_round_trips_and_runs(sg, tmp_path):
    out = tmp_path / "summaries.py"
    tricky = 'quotes " and "" and """ backslash \\ unicode é {braces}\nline two'
    data = {
        "generated": "now",
        "model": "m",
        "source_directory": str(tmp_path),
        "repos": {"alpha": {"summary": tricky, "hash": "h"}},
    }
    sg.write_output(str(out), data)
    assert sg.load_previous(str(out)) == data["repos"]
    assert os.access(out, os.X_OK)
    assert not out.with_name("summaries.py.tmp").exists()
    listing = subprocess.run(
        [sys.executable, str(out), "--list"], capture_output=True, text=True, check=True
    )
    assert listing.stdout.split() == ["alpha"]
    shown = subprocess.run(
        [sys.executable, str(out), "alpha"], capture_output=True, text=True, check=True
    )
    assert "unicode é {braces}" in shown.stdout


def test_load_previous_tolerates_bad_files(sg, tmp_path, monkeypatch):
    # The script binds stderr at import time, so capture it directly.
    errors = io.StringIO()
    monkeypatch.setattr(sg, "stderr", errors)
    assert sg.load_previous(str(tmp_path / "missing.py")) == {}
    other = tmp_path / "other.py"
    other.write_text("print('hi')\n")
    assert sg.load_previous(str(other)) == {}
    assert "not a summaries file" in errors.getvalue()


def test_gather_context_collects_in_priority_order(sg, tmp_path):
    repo = tmp_path / "repo"
    (repo / ".github" / "workflows").mkdir(parents=True)
    (repo / "sub").mkdir()
    (repo / "node_modules" / "pkg").mkdir(parents=True)
    (repo / "README.rst").write_text("Repo\n====\n\nIt does things.\n")
    (repo / "Makefile").write_text("all:\n\ttrue\n")
    (repo / "LICENSE").write_text("license text " * 100)  # over its 400-char slice
    (repo / ".github" / "workflows" / "ci.yml").write_text("on: push\n")
    (repo / "sub" / "README.md").write_text("# Sub\n")
    (repo / "node_modules" / "pkg" / "README.md").write_text("# skipped\n")

    context, truncated, readme = sg.gather_context(str(repo), 65536)
    titles = [
        line[len("----- FILE: ") : -len(" -----")]
        for line in context.splitlines()
        if line.startswith("----- FILE: ")
    ]
    assert titles == [
        "README.rst",
        "LICENSE",
        "Makefile",
        ".github/workflows/ci.yml",
        "Directory layout (top two levels)",
        "sub/README.md",
    ]
    assert readme == "README.rst"
    assert not truncated  # a sliced LICENSE head is not truncated input
    assert "skipped" not in context
    assert context.count(sg.TRUNC_MARK) == 1  # the LICENSE slice is marked inline


def test_gather_context_fallbacks_and_budget(sg, tmp_path):
    assert sg.gather_context(str(tmp_path), 65536) == ("", False, "")

    docs_only = tmp_path / "docs_only"
    (docs_only / "docs").mkdir(parents=True)
    (docs_only / "docs" / "README.md").write_text("# Docs\n")
    context, truncated, readme = sg.gather_context(str(docs_only), 65536)
    assert readme == "docs/README.md" and "# Docs" in context

    big = tmp_path / "big"
    big.mkdir()
    (big / "README.md").write_text("word " * 20000)
    (big / "Makefile").write_text("all:\n\ttrue\n" * 100)
    context, truncated, _ = sg.gather_context(str(big), 8000)
    assert truncated and len(context) <= 8000


def test_stream_chat_parses_sse_and_strips_thinking(sg):
    session = FakeSession(
        [FakeResponse(200, sse("<think>hmm</think>", "Hello ", "world"))]
    )
    content, finish = sg.stream_chat(session, "u", {}, None)
    assert (content, finish) == ("Hello world", "stop")

    session = FakeSession([FakeResponse(503, text='{"error": "loading"}')])
    with pytest.raises(sg.LLMHTTPError) as info:
        sg.stream_chat(session, "u", {}, None)
    assert info.value.status == 503 and "loading" in info.value.body


def test_pull_ai_retries_only_retryable_errors(sg, monkeypatch):
    monkeypatch.setattr(sg.time, "sleep", lambda s: None)
    options = {"model": "m", "max_tokens": 100, "timeout": 0, "retries": 3, "url": "u"}

    session = FakeSession(
        [FakeResponse(503, text="busy"), FakeResponse(200, sse("ok", finish="length"))]
    )
    assert sg.pull_ai(session, options, "repo", "ctx") == ("ok", True)
    assert len(session.calls) == 2
    assert session.calls[0]["stream"] is True
    assert "Project name: repo" in session.calls[0]["messages"][1]["content"]

    session = FakeSession([FakeResponse(400, text="bad")])
    with pytest.raises(RuntimeError, match="HTTP 400: bad"):
        sg.pull_ai(session, options, "repo", "ctx")
    assert len(session.calls) == 1

    session = FakeSession([FakeResponse(200, sse("")) for _ in range(3)])
    with pytest.raises(RuntimeError, match="empty response"):
        sg.pull_ai(session, options, "repo", "ctx")
    assert len(session.calls) == 3


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_git_info_scrubs_remote(sg, tmp_path):
    assert sg.git_info(str(tmp_path)) == {}
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "README.md").write_text("# r\n")
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t.t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t.t",
        "PATH": os.environ["PATH"],
    }
    for cmd in (
        ["git", "init", "-q", "-b", "main"],
        ["git", "add", "README.md"],
        ["git", "commit", "-q", "-m", "first commit"],
        ["git", "remote", "add", "origin", "https://u:secret@example.com/r.git"],
    ):
        subprocess.run(cmd, cwd=repo, check=True, env=env, capture_output=True)
    info = sg.git_info(str(repo))
    assert info["remote"] == "https://example.com/r.git"
    assert info["branch"] == "main"
    assert info["commit"].endswith("first commit")
