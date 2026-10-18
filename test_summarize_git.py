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


# --------------------------------------------------------------------------- #
# Recursive discovery
# --------------------------------------------------------------------------- #


def make_tree(root):
    """A nested fixture with every kind of unit --recursive knows.

    alpha/              git repo (README, Makefile)
      sub/              submodule (.git file -> .git/modules/sub)
      tools/cli/        directory with a README; tools/ itself has none
      node_modules/pkg/ README that must be skipped
      build/            README that is skipped unless --include build
    wt/                 worktree (.git file -> .git/worktrees/wt)
    mirror.git/         bare repo; the README under hooks/ must never surface
    group/beta/         directory with a README; group/ has none
    group/gamma/        Makefile only: a unit only with --build-dirs
    noreadme/           git repo without a README
      inner/            directory with a README
    docsonly/docs/      README picked up by the docs/ fallback; docs/ is no unit
    .hidden/            hidden, skipped
    """
    files = {
        "alpha/README.md": "# Alpha\n\n[![ci](a)](b)\n\nAlpha is the top-level project.\n",
        "alpha/Makefile": "all:\n\ttrue\n",
        "alpha/sub/.git": "gitdir: ../.git/modules/sub\n",
        "alpha/sub/README.md": "# Sub\n\nSub provides widgets.\n",
        "alpha/tools/cli/README.rst": "CLI\n===\n\nThe command line for alpha.\n",
        "alpha/node_modules/pkg/README.md": "# pkg\n",
        "alpha/build/README.md": "# build output\n",
        "wt/.git": "gitdir: /elsewhere/repo/.git/worktrees/wt\n",
        "wt/README.md": "# WT\n\nA worktree.\n",
        "mirror.git/HEAD": "ref: refs/heads/main\n",
        "mirror.git/config": "[core]\n\tbare = true\n",
        "mirror.git/hooks/README.md": "# never seen\n",
        "group/beta/README.rst": "Beta\n----\n\nBeta lives in a group.\n",
        "group/gamma/Makefile": "all:\n\ttrue\n",
        "noreadme/inner/README.md": "# Inner\n\nInner part of a README-less repo.\n",
        "docsonly/docs/README.md": "# Docs only\n\nDocumented under docs.\n",
        ".hidden/README.md": "# hidden\n",
    }
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    for rel in ("alpha/.git", "noreadme/.git", "mirror.git/objects", "mirror.git/refs"):
        (root / rel).mkdir(parents=True)
    return root


TREE_NAMES = [
    "alpha",
    "alpha/sub",
    "alpha/tools/cli",
    "docsonly",
    "group/beta",
    "noreadme/inner",
    "wt",
]


def names_of(units):
    return [unit.name for unit in units]


def test_discover_units_finds_every_kind_in_tree_order(sg, tmp_path):
    units = sg.discover_units(str(make_tree(tmp_path)))
    assert [(u.name, u.kind) for u in units] == [
        ("alpha", "repo"),
        ("alpha/sub", "submodule"),
        ("alpha/tools/cli", "directory"),
        ("docsonly", "directory"),
        ("group/beta", "directory"),
        ("mirror.git", "bare"),
        ("noreadme", "repo"),
        ("noreadme/inner", "directory"),
        ("wt", "worktree"),
    ]
    by = {u.name: u for u in units}
    assert by["docsonly"].readme == "docs/README.md"
    assert by["mirror.git"].readme == "" and by["noreadme"].readme == ""
    assert by["alpha"].build and not by["alpha/sub"].build


def test_discover_units_assigns_nearest_enclosing_parent(sg, tmp_path):
    units = sg.discover_units(str(make_tree(tmp_path)))
    tree = {u.name: (u.parent, u.depth, u.children) for u in units}
    assert tree["alpha"] == (None, 0, ["alpha/sub", "alpha/tools/cli"])
    assert tree["alpha/sub"] == ("alpha", 1, [])
    assert tree["alpha/tools/cli"] == ("alpha", 1, [])  # tools/ is no unit
    assert tree["group/beta"] == (None, 0, [])  # neither is group/
    assert tree["noreadme"] == (None, 0, ["noreadme/inner"])
    assert tree["noreadme/inner"] == ("noreadme", 1, [])  # a parent needs no README


def test_discover_units_build_dirs_exclude_and_include(sg, tmp_path):
    root = str(make_tree(tmp_path))
    assert "group/gamma" not in names_of(sg.discover_units(root))
    with_build = names_of(sg.discover_units(root, build_dirs=True))
    assert "group/gamma" in with_build
    assert "alpha/build" not in with_build  # skipped directories stay skipped
    skip = frozenset((set(sg.SKIP_DIRS) | {"tools"}) - {"build"})
    adjusted = names_of(sg.discover_units(root, skip))
    assert "alpha/build" in adjusted
    assert "alpha/tools/cli" not in adjusted
    assert "alpha/node_modules/pkg" not in adjusted
    assert "mirror.git/hooks" not in adjusted


def test_git_file_kind(sg, tmp_path):
    dotgit = tmp_path / ".git"
    dotgit.write_text("gitdir: C:\\repo\\.git\\worktrees\\wt\n")
    assert sg.git_file_kind(str(dotgit)) == "worktree"
    dotgit.write_text("gitdir: ../.git/modules/sub\n")
    assert sg.git_file_kind(str(dotgit)) == "submodule"
    assert sg.git_file_kind(str(tmp_path / "missing")) == "submodule"


def titles_in(context):
    return [
        line[len("----- FILE: ") : -len(" -----")]
        for line in context.splitlines()
        if line.startswith("----- FILE: ")
    ]


def test_gather_context_leaves_child_units_readmes_out(sg, tmp_path):
    root = make_tree(tmp_path)
    stop = frozenset(u.path for u in sg.discover_units(str(root)))
    alpha = str(root / "alpha")
    flat, _, _ = sg.gather_context(alpha, 65536)
    assert "tools/cli/README.rst" in titles_in(flat)  # flat mode, as before
    nested, _, _ = sg.gather_context(alpha, 65536, sg.SKIP_DIRS, stop)
    assert "tools/cli/README.rst" not in titles_in(nested)
    assert "sub/README.md" not in titles_in(nested)

    # A --build-dirs unit is summarized from its build files alone
    gamma = str(root / "group" / "gamma")
    assert sg.gather_context(gamma, 65536) == ("", False, "")
    context, truncated, readme = sg.gather_context(gamma, 65536, require_readme=False)
    assert titles_in(context)[0] == "Makefile" and readme == "" and not truncated
    empty = tmp_path / "empty"
    empty.mkdir()
    assert sg.gather_context(str(empty), 65536, require_readme=False) == ("", False, "")


def test_readme_excerpt_skips_headings_badges_and_html(sg, tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text(
        "# Title\n\n[![ci](a)](b)\n\n<p align=center>x</p>\n\n"
        "First real\nparagraph here.\n\nSecond.\n"
    )
    assert sg.readme_excerpt(str(readme)) == "First real paragraph here."
    readme.write_text("Title\n=====\n\n" + "word " * 100)
    excerpt = sg.readme_excerpt(str(readme), limit=40)
    assert excerpt.endswith("...") and len(excerpt) <= 43
    readme.write_text("# Only a title\n")
    assert sg.readme_excerpt(str(readme)) == "Only a title"
    assert sg.readme_excerpt(str(tmp_path / "missing")) == ""


def test_structure_note_describes_parent_and_children(sg, tmp_path):
    units = sg.discover_units(str(make_tree(tmp_path)))
    by = {u.name: u for u in units}
    assert sg.structure_note(by["alpha"], by) == (
        "Sub-components inside this project, each summarized separately:\n"
        "- sub (git submodule): Sub provides widgets.\n"
        "- tools/cli (directory): The command line for alpha."
    )
    assert sg.structure_note(by["alpha/sub"], by) == (
        "Parent: this project is the git submodule at 'sub' inside the git "
        "repository 'alpha'."
    )
    assert sg.structure_note(by["wt"], by) == ""
    assert by["alpha/tools/cli"].project == "cli"


def test_flat_cache_keys_are_unchanged(sg):
    # Computed before --recursive existed. A different digest here would
    # invalidate every flat-mode cache out there.
    assert (
        sg.cache_key("m", "n", "ctx")
        == "2acef349de20c14e4ca71621b91bc0e75164098c92c742bac2fc8ba5bab01cd9"
    )
    assert sg.cache_key("m", "n", "ctx", "") == sg.cache_key("m", "n", "ctx")
    assert sg.cache_key("m", "n", "ctx", "Parent: x") != sg.cache_key("m", "n", "ctx")
    assert sg.USER_PROMPT == sg.USER_PROMPT_HEAD + sg.USER_PROMPT_TAIL


def test_pull_ai_adds_structure_prompt_only_when_given(sg):
    options = {"model": "m", "max_tokens": 100, "timeout": 0, "retries": 1, "url": "u"}
    session = FakeSession([FakeResponse(200, sse("ok"))])
    sg.pull_ai(session, options, "repo", "ctx")
    plain = session.calls[0]["messages"][1]["content"]
    assert plain == sg.USER_PROMPT.format(name="repo", context="ctx")

    session = FakeSession([FakeResponse(200, sse("ok"))])
    sg.pull_ai(session, options, "repo", "ctx {braces}", "Parent: {x}")
    content = session.calls[0]["messages"][1]["content"]
    assert "Parent: {x}\n\nProject name: repo\n\n" in content
    assert content.endswith("ctx {braces}")


def run_main(sg, monkeypatch, capsys, *argv):
    """Run main() with argv, no config file and no calls to an LLM server."""
    monkeypatch.setattr(sg, "model_status", lambda *args: ("", 0))
    monkeypatch.setattr(sg, "check_model", lambda *args: None)
    monkeypatch.setattr(sys, "argv", ["summarize_git.py", "-c", "/no/such/cfg", *argv])
    code = sg.main()
    return code, capsys.readouterr().out


def viewer(out_path, *argv, stdin=""):
    result = subprocess.run(
        [sys.executable, str(out_path), *argv],
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def test_dry_run_lists_tree_with_kinds(sg, tmp_path, monkeypatch, capsys):
    root = str(make_tree(tmp_path))
    code, out = run_main(sg, monkeypatch, capsys, "-d", root, "-r", "-n")
    assert code == 0
    lines = out.splitlines()
    assert lines[0] == f"Found 9 units under {root}, 7 with something to summarize"
    assert lines[-1] == "0 cached, 7 to summarize, 2 skipped"
    assert "  new      repo      alpha  (README.md, " in out
    assert "  new      submodule   alpha/sub  (README.md, " in out
    assert "  new      directory   alpha/tools/cli  (README.rst, " in out
    assert "  skipped  bare      mirror.git  (no README)" in out
    assert "  skipped  repo      noreadme  (no README)" in out
    assert "  new      directory   noreadme/inner  (README.md, " in out
    assert "  new      worktree  wt  (README.md, " in out
    for absent in ("hooks", ".hidden", "node_modules", "alpha/build", "gamma"):
        assert absent not in out

    # Flat mode prints what it always has
    code, out = run_main(sg, monkeypatch, capsys, "-d", root, "-n")
    assert code == 0
    lines = out.splitlines()
    assert lines[0] == f"Found 3 repos with a README under {root}"
    assert lines[1].startswith("  new      alpha  (README.md, ")
    assert lines[-1] == "0 cached, 3 to summarize"


def test_recursive_run_writes_viewer_with_tree_links(sg, tmp_path, monkeypatch, capsys):
    root = make_tree(tmp_path)
    out_path = tmp_path / "out.py"
    monkeypatch.setattr(
        sg,
        "pull_ai",
        lambda session, options, name, context, structure="": (
            f"Summary of {name}.",
            False,
        ),
    )
    argv = ("-d", str(root), "-r", "-q", "-o", str(out_path))
    code, out = run_main(sg, monkeypatch, capsys, *argv)
    assert code == 0 and out.count(": summarizing") == 7

    repos = sg.load_previous(str(out_path))
    assert sorted(repos) == sorted(TREE_NAMES)
    assert repos["alpha"]["kind"] == "repo" and repos["alpha"]["depth"] == 0
    assert repos["alpha"]["children"] == ["alpha/sub", "alpha/tools/cli"]
    assert "parent" not in repos["alpha"]
    assert repos["alpha/sub"]["parent"] == "alpha" and repos["alpha/sub"]["depth"] == 1
    assert repos["alpha/sub"]["kind"] == "submodule"
    assert repos["alpha/sub"]["summary"] == "Summary of sub."
    assert repos["noreadme/inner"]["parent"] == "noreadme"  # parent has no entry

    assert viewer(out_path, "--list").split() == TREE_NAMES
    shown = viewer(out_path, "alpha")
    assert "Kind: repo" in shown and "Contains: sub, tools/cli" in shown
    shown = viewer(out_path, "cli")  # a nested unit by its last path component
    assert "alpha/tools/cli" in shown and "Parent: alpha" in shown
    assert "Wrote 7 summaries" in viewer(out_path, "--text", str(tmp_path / "s.txt"))
    assert "Parent: alpha" in (tmp_path / "s.txt").read_text()
    assert json.loads(viewer(out_path, "--json"))["repos"] == repos
    menu = viewer(out_path, stdin="q\n")
    assert (
        "1. alpha\n2.   sub\n3.   tools/cli\n4. docsonly\n5. group/beta\n"
        "6.   noreadme/inner\n7. wt\n"
    ) in menu

    # A second run is served from the cache; a flat run on the same file keeps
    # the unchanged top-level entries and drops the tree fields again.
    code, out = run_main(sg, monkeypatch, capsys, *argv)
    assert code == 0 and out.count(": cached") == 7
    code, out = run_main(
        sg, monkeypatch, capsys, "-d", str(root), "-q", "-o", str(out_path)
    )
    assert code == 0 and out.count(": cached") == 2  # alpha's context changed
    repos = sg.load_previous(str(out_path))
    assert sorted(repos) == ["alpha", "docsonly", "wt"]
    assert not any(key in repos["wt"] for key in sg.TREE_KEYS)
