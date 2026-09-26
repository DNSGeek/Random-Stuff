#!/usr/bin/env python3
# The MIT License (MIT)
#
# Copyright (c) 2015-2021 Steven Fernandez
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# CLEAN_NS must be captured before any imports, so imports can't be at the top.
# ruff: noqa: E402

# Keep a copy of the initial namespace, we'll need it later
CLEAN_NS = globals().copy()

"""pymp - lonetwin's pimped-up pythonrc

This file will be executed when the Python interactive shell is started, if
$PYTHONSTARTUP is in your environment and points to this file. You could
also make this file executable and call it directly.

This file creates an InteractiveConsole instance, which provides:
  * execution history
  * colored prompts and pretty printing
  * auto-indentation
  * intelligent tab completion:¹
  * source code listing for objects
  * session history editing using your $EDITOR, as well as editing of
    source files for objects or regular files
  * temporary escape to $SHELL or ability to execute a shell command and
    capturing the result into the '_' variable
  * convenient printing of doc stings and search for entries in online docs
  * auto-execution of a virtual env specific (`.venv_rc.py`) file at startup
  * auto-import of undefined names that match a top-level module
  * an asyncio event loop with a nested REPL where top-level await works

If you have any other good ideas please feel free to submit issues/pull requests.

¹ Since python 3.4 the default interpreter also has tab completion
enabled however it does not do pathname completion
"""

# - Exit if being called from within ipython
try:
    import sys

    __IPYTHON__ and sys.exit(0)  # type: ignore
except NameError:
    pass


# Import policy: only modules needed to build the console and show the first
# prompt are imported here. Everything else is imported inside the function
# that uses it. After the first call an import is just a sys.modules dict
# lookup, so the runtime cost is noise, while the startup saving is real
# (asyncio alone is ~35ms, and nothing touches it unless you type \A).
#
# `rlcompleter` has to stay because ImprovedCompleter subclasses it at class
# definition time. It drags in `inspect` and `keyword`, so deferring those two
# would save nothing.
import atexit
import keyword
import os
import re
import readline
import rlcompleter
from code import InteractiveConsole
from functools import cached_property, lru_cache, partial, wraps
from types import SimpleNamespace

__version__ = "0.10.0"

# Pre-compiled regex constants - kept at module level to avoid recompilation
# - dict keys in pprint output: a quoted string or a parenthesised tuple
# followed by ': ', at the start of a line or right after '{' or ', '. Every
# alternative is linear (no nested quantifiers), so a big dict of quote-heavy
# strings can't make the colouring pass take seconds.
_RE_DICT_KEYS = re.compile(r"""(?<![^\s{,])('[^'\n]*'|"[^"\n]*"|\([^()\n]*\)): """)
# - {name} / {dotted.name} fields in shell commands that get expanded from
# the namespace. Anything else between braces is left for the shell.
_RE_SHELL_FIELD = re.compile(r"\{([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)\}")


def _path_signature() -> tuple:
    """Cheap fingerprint of sys.path: each entry with its mtime.

    A ``pip install`` into site-packages changes that directory's mtime, so
    keying the module index on this makes newly installed packages visible
    to completion and auto-import without an explicit refresh. A dozen
    stat() calls is noise next to a readline round trip.
    """
    signature = []
    for entry in sys.path:
        try:
            mtime = os.stat(entry or os.getcwd()).st_mtime_ns
        except OSError:
            mtime = None
        signature.append((entry, mtime))
    return tuple(signature)


@lru_cache(maxsize=4)
def _scan_modules(signature) -> tuple[frozenset[str], frozenset[str]]:
    """One filesystem scan returning (packages, modules) on the module path.

    pkgutil.iter_modules() walks the entire module path, so both sets are
    built in a single pass. *signature* is only used as the cache key.
    """
    import pkgutil

    pkgs: set[str] = set()
    mods: set[str] = set()
    for item in pkgutil.iter_modules():
        if not item.name.startswith("_"):
            mods.add(item.name)
            if item.ispkg:
                pkgs.add(item.name)
    for name in sys.builtin_module_names:
        if not name.startswith("_"):
            mods.add(name)
    return frozenset(pkgs), frozenset(mods)


@lru_cache(maxsize=256)
def _submodules(parent: str, signature) -> list[str]:
    """Direct children of package *parent*, as dotted names.

    Lists one level with pkgutil.iter_modules() instead of walking the whole
    tree with walk_packages(): the latter imports every sub-package it
    visits, which runs arbitrary code and can take seconds on numpy-sized
    packages. find_spec() still imports *parent* itself, which the user is
    about to do anyway. *signature* is only used as the cache key.
    """
    import importlib.util
    import pkgutil

    try:
        spec = importlib.util.find_spec(parent)
    except Exception:
        # find_spec imports the parent packages of a dotted name, and any
        # of them may fail. A completer must never raise.
        return []
    if spec is None or not spec.submodule_search_locations:
        return []
    return [
        item.name
        for item in pkgutil.iter_modules(spec.submodule_search_locations, f"{parent}.")
    ]


config = SimpleNamespace(
    ONE_INDENT="    ",  # what should we use for indentation ?
    HISTFILE=os.path.expanduser("~/.python_history"),
    # - max number of entries kept in HISTFILE. readline truncates the file
    # to this many entries every time it writes it (see set_history_length
    # in init_readline), so the file can't grow without bound. Set to -1 for
    # unlimited.
    HISTSIZE=10000,
    EDITOR=os.getenv("EDITOR", "vi"),
    SHELL=os.getenv("SHELL", "/bin/bash"),
    EDIT_CMD=r"\e",
    SH_EXEC="!",
    DOC_CMD="?",
    DOC_URL="https://docs.python.org/{sys.version_info.major}/search.html?q={term}",
    HELP_CMD=r"\h",
    LIST_CMD=r"\l",
    AUTO_INDENT=True,  # - Should we auto-indent by default
    VENV_RC=os.getenv("VENV_RC", ".venv_rc.py"),
    # - option to pass to the editor to open a file at a specific
    # `line_no`. This is used when the EDIT_CMD is invoked with a python
    # object to open the source file for the object.
    LINE_NUM_OPT="+{line_no}",
    # - Run-time toggle for auto-indent command (eg: when pasting code)
    TOGGLE_AUTO_INDENT_CMD=r"\\",
    # - should path completion expand ~ using os.path.expanduser()
    COMPLETION_EXPANDS_TILDE=True,
    # - when executing edited history, should we also print comments
    POST_EDIT_PRINT_COMMENTS=True,
    # - Attempt to auto-import top-level module names on NameError
    ENABLE_AUTO_IMPORTS=True,
    # - Start/Stop the asyncio loop in the interpreter (similar to `python -m asyncio`)
    TOGGLE_ASYNCIO_LOOP_CMD=r"\A",
    # - colour output. None = auto: only when stdout is a tty, NO_COLOR is
    # unset and TERM is not "dumb". True/False force it on/off.
    USE_COLOR=None,
    # - pprint sorts dict keys by default; False keeps insertion order, like
    # repr() does.
    PPRINT_SORT_DICTS=False,
    # - top-level modules that are never auto-imported on NameError. A typo
    # that resolves to `antigravity` opens a browser and `this` prints the
    # Zen of Python, which is not what anyone wants from a typo.
    AUTO_IMPORT_DENYLIST=frozenset({"antigravity", "this"}),
)

# Color functions. These get initialized in init_color_functions() later
red = green = yellow = blue = purple = cyan = grey = str


class ImprovedCompleter(rlcompleter.Completer):
    """A smarter rlcompleter.Completer"""

    def __init__(self, namespace=None):
        super().__init__(namespace)
        # - remove '/' and '~' from delimiters to help with path completion
        completer_delims = readline.get_completer_delims()
        completer_delims = completer_delims.replace("/", "")
        if config.COMPLETION_EXPANDS_TILDE:
            completer_delims = completer_delims.replace("~", "")
        readline.set_completer_delims(completer_delims)
        self.matches = []

    # The module index is cached at module level, keyed on a fingerprint of
    # sys.path, so it is rebuilt only when the path (or a directory on it)
    # changes - eg: after a `pip install` from the shell command.
    @property
    def pkglist(self) -> frozenset[str]:
        return _scan_modules(_path_signature())[0]

    @property
    def modlist(self) -> frozenset[str]:
        return _scan_modules(_path_signature())[1]

    def submodules(self, parent: str) -> list[str]:
        """Direct children of package *parent* as dotted names."""
        return _submodules(parent, _path_signature())

    def module_matches(self, text: str) -> list[str]:
        """Complete a (possibly dotted) module name that is being typed."""
        if "." in text:
            parent, _, _ = text.rpartition(".")
            return self.startswith_filter(text, self.submodules(parent))
        return self.startswith_filter(text, self.modlist)

    @cached_property
    def exception_names(self) -> list[str]:
        """Walk the full exception hierarchy iteratively and cache the result.

        Multiple inheritance means a class can be reached by more than one
        path, so track visited classes to avoid duplicate completions.
        """
        names: dict[str, None] = {}
        seen: set[type] = set()
        stack = [Exception]
        while stack:
            cls = stack.pop()
            if cls in seen:
                continue
            seen.add(cls)
            names[cls.__name__] = None
            stack.extend(cls.__subclasses__())
        return list(names)

    def startswith_filter(self, text, names, striptext=None):
        # Must return a list, not a generator: complete() calls len() on the
        # result, indexes it by readline's `state`, and may .extend() it.
        if striptext:
            return [
                name.removeprefix(striptext) for name in names if name.startswith(text)
            ]
        return [name for name in names if name.startswith(text)]

    def get_path_matches(self, text):
        import glob

        # Use a single '*' glob for one-level completion. The bare '**' pattern
        # without recursive=True does not descend into subdirectories and is
        # misleading; readline tab-completion is single-level by convention.
        # glob.escape() so a literal '[', '?' or '*' in the typed path is
        # treated as a character rather than a glob metacharacter.
        return [
            f"{item}{os.path.sep}" if os.path.isdir(item) else item
            for item in glob.iglob(f"{glob.escape(text)}*")
        ]

    def get_import_matches(self, text, words):
        import importlib

        if words[0] == "import":
            # import p<tab> / import pkg.su<tab> / import a, b<tab>
            if text or len(words) == 1:
                return self.module_matches(text)
            return []

        # - everything below is a `from ...` line
        if len(words) == 1 or (len(words) == 2 and text):
            # from p<tab> / from pkg.su<tab>
            return self.module_matches(text)

        if (len(words) == 2 and not text) or (
            len(words) == 3 and text and "import".startswith(text)
        ):
            # from pkg <tab> / from pkg im<tab>
            return ["import "]

        if len(words) >= 3 and words[2] == "import":
            # from pkg.sub import na<tab>: the package's direct sub-modules
            # (one level, so no `sub.deeper` names that are invalid here)
            # plus the names the module itself defines.
            namespace = words[1]
            prefix = f"{namespace}."
            matches = self.startswith_filter(
                prefix + text, self.submodules(namespace), striptext=prefix
            )
            # Importing here runs arbitrary module-level code, which may fail
            # for any reason. A completer must never raise.
            try:
                mod = importlib.import_module(namespace)
            except Exception:
                return matches
            names = getattr(mod, "__all__", None) or dir(mod)
            seen = set(matches)
            for name in names:
                if isinstance(name, str) and name.startswith(text):
                    if name not in seen:
                        seen.add(name)
                        matches.append(name)
            return matches

        return []

    def complete(self, text, state, line=None):
        if not line:
            line = readline.get_line_buffer()

        if line == "" or line.isspace():
            return None if state else config.ONE_INDENT

        words = line.split()
        if state == 0:
            # - this is the first completion is being attempted for
            # text, we need to populate self.matches, just like
            # super().complete()
            if line.startswith(("from ", "import ")):
                self.matches = self.get_import_matches(text, words) or []
            elif words[0] in ("raise", "except"):
                self.matches = self.startswith_filter(
                    text.lstrip("("), self.exception_names
                )
            elif os.path.sep in text:
                self.matches = self.get_path_matches(
                    os.path.expanduser(text)
                    if config.COMPLETION_EXPANDS_TILDE
                    else text
                )
            elif "." in text:
                self.matches = self.attr_matches(text)
            else:
                self.matches = self.global_matches(text)

            if len(self.matches) == 1:
                # - a single directory match: descend into it right away
                match = self.matches[0]
                if match and match.endswith(os.path.sep):
                    self.matches.extend(self.get_path_matches(match))

        try:
            return self.matches[state]
        except IndexError:
            return None


def _doc_to_usage(method):
    @wraps(method)
    def inner(self, arg):
        arg = arg.strip()
        if arg.split(None, 1)[:1] in (["-h"], ["--help"]):
            # The docstrings are templates referencing `config`, so they have
            # to be formatted before display or the user sees raw braces.
            usage = method.__doc__.format(config=config).strip()
            return self.writeline(blue(usage))
        return method(self, arg)

    return inner


class ImprovedConsole(InteractiveConsole):
    """
    Welcome to lonetwin's pimped up python prompt

    You've got color, tab completion, auto-indentation, pretty-printing
    and more !

    * A tab with preceding text will attempt auto-completion of
      keywords, names in the current namespace, attributes and methods.
      If the preceding text has a '/', filename completion will be
      attempted. Without preceding text four spaces will be inserted.

    * History will be saved in {HISTFILE} when you exit.

    * If you create a file named {VENV_RC} in the current directory, the
      contents will be executed in this session before the prompt is
      shown.

    * Typing out a defined name followed by a '{DOC_CMD}' will print out
      the object's __doc__ attribute if one exists.
      (eg: []? / str? / os.getcwd? )

    * Typing '{DOC_CMD}{DOC_CMD}' after something will search for the
      term at {DOC_URL}
      (eg: try webbrowser.open??)

    * Open your editor with current session history, source code of
      objects or arbitrary files, using the '{EDIT_CMD}' command.

    * List source code for objects using the '{LIST_CMD}' command.

    * Execute shell commands using the '{SH_EXEC}' command, or
      '{SH_EXEC}{SH_EXEC}' for interactive ones (editors, pagers, ...).

    * Toggle auto-indentation (eg: before pasting a block of code) with
      the '{TOGGLE_AUTO_INDENT_CMD}' command.

    * Start an asyncio event loop, with a nested REPL in which top-level
      'await' works, using the '{TOGGLE_ASYNCIO_LOOP_CMD}' command.

    * An undefined name that matches a top-level module is imported
      automatically and the statement re-run (eg: `os.getcwd()` without a
      prior `import os`). ENABLE_AUTO_IMPORTS in the config turns this off.

    Try `<cmd> -h`, or '{HELP_CMD} <cmd>', for any of the commands to learn
    more.

    The EDITOR, SHELL, command names and more can be changed in the
    config declaration at the top of this file. Make this your own !
    """

    def runcode_sync(self, code):
        """Wrapper around super().runcode() to enable auto-importing"""
        if not config.ENABLE_AUTO_IMPORTS:
            return super().runcode(code)

        try:
            exec(code, self.locals)
        except NameError as err:
            if self._auto_import(err, code):
                return self.runcode(code)
            self.showtraceback()
        except SystemExit:
            raise
        except Exception:
            self.showtraceback()

    def _auto_import(self, err, code):
        """Import the module an undefined name refers to, if there is one.

        Returns True when the module was bound in the namespace and the
        statement should be re-run.
        """
        import importlib

        # Auto-importing re-executes the whole statement, so only do it
        # when the NameError came from the top-level statement itself.
        # If it was raised inside a called function, that function has
        # already run and re-running it would repeat its side effects.
        tb = err.__traceback__
        while tb.tb_next is not None:
            tb = tb.tb_next
        if tb.tb_frame.f_code is not code:
            return False

        # NameError.name (python 3.10+) is None when the error was raised
        # by hand rather than by a failed name lookup.
        name = getattr(err, "name", None)
        if (
            not name
            or name in config.AUTO_IMPORT_DENYLIST
            or name not in self.completer.modlist
        ):
            return False
        try:
            mod = importlib.import_module(name)
        except Exception as exc:
            # Anything can go wrong at import time, not just ImportError,
            # and an exception escaping this handler would crash the session.
            print(grey(f"# auto-import of {name} failed: {exc!r}", bold=False))
            return False
        print(
            grey(
                f"# imported undefined module: {name} (re-running statement)",
                bold=False,
            )
        )
        self.locals[name] = mod
        return True

    runcode = runcode_sync

    def __init__(self, *args, **kwargs):
        self.session_history = []  # This holds the last executed statements
        self.buffer = []  # This holds the statement to be executed
        self._indent = ""
        self._skip_subsequent = False
        self._venv_rc_status = None
        self.loop = None
        self.repl_thread = None
        super().__init__(*args, **kwargs)

        self.init_color_functions()
        self.init_readline()
        self.init_prompt()
        self.init_pprint()
        # - dict mapping commands to their handler methods
        self.commands = {
            config.EDIT_CMD: self.process_edit_cmd,
            config.LIST_CMD: self.process_list_cmd,
            config.SH_EXEC: self.process_sh_cmd,
            config.HELP_CMD: self.process_help_cmd,
            config.TOGGLE_AUTO_INDENT_CMD: self.toggle_auto_indent,
            config.TOGGLE_ASYNCIO_LOOP_CMD: self.toggle_asyncio,
        }
        # - regex to identify and extract commands and their arguments.
        # Everything after the command is its argument, parentheses
        # included: `!python -c "print(1)"` must reach the shell intact.
        self.commands_re = re.compile(
            r"(?P<cmd>{})\s*(?P<args>.*)".format(
                "|".join(re.escape(cmd) for cmd in self.commands)
            )
        )

    def init_color_functions(self):
        """Populates globals dict with some helper functions for colorizing text"""
        use_color = config.USE_COLOR
        if use_color is None:
            # - honour https://no-color.org and keep escape codes out of
            # pipes and dumb terminals
            isatty = getattr(sys.stdout, "isatty", None)
            use_color = (
                isatty is not None
                and isatty()
                and os.getenv("NO_COLOR") is None
                and os.getenv("TERM") != "dumb"
            )

        def colorize(color_code, text, bold=True, readline_workaround=False):
            reset = "\033[0m"
            color = "\033[{}{}m".format("1;" if bold else "", color_code)
            # - reason for readline_workaround: http://bugs.python.org/issue20359
            if readline_workaround:
                return f"\001{color}\002{text}\001{reset}\002"
            return f"{color}{text}{reset}"

        def plain(color_code, text, bold=True, readline_workaround=False):
            return str(text)

        g = globals()
        for code, color in enumerate(
            ["red", "green", "yellow", "blue", "purple", "cyan", "grey"], 31
        ):
            g[color] = partial(colorize if use_color else plain, code)

    def init_readline(self):
        """Activates history and tab completion"""
        # - 1. history stuff
        # - mainly borrowed from site.enablerlcompleter() from py3.4+,
        # we can't simply call site.enablerlcompleter() because its
        # implementation overwrites the history file for each python
        # session whereas we prefer appending history from every
        # (potentially concurrent) session.

        # Reading the initialization (config) file may not be enough to set a
        # completion key, so we set one first and then read the file.
        # readline.backend is python 3.13+; older versions only reveal
        # libedit through the module docstring.
        backend = getattr(readline, "backend", None)
        if backend is None:
            backend = (
                "editline" if "libedit" in (readline.__doc__ or "") else "readline"
            )
        if backend == "editline":
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")

        try:
            readline.read_init_file()
        except OSError:
            # An OSError here could have many causes, but the most likely one
            # is that there's no .inputrc file (or .editrc file in the case of
            # Mac OS X + libedit) in the expected location.  In that case, we
            # want to ignore the exception.
            pass

        # macOS ships libedit instead of GNU readline. libedit does not
        # implement append_history_file, so we fall back to rewriting the
        # full history file on exit (same behaviour as the default site
        # module, but still better than crashing with an AttributeError).
        # Either way readline itself truncates the file to HISTSIZE entries
        # on every write (see set_history_length below), so nothing else
        # needs to trim it - and must not: libedit's file starts with a
        # marker line that a naive line-based trim would drop.
        _has_append_history = hasattr(readline, "append_history_file")

        def append_history(len_at_start):
            current_len = readline.get_current_history_length()
            if _has_append_history:
                try:
                    readline.append_history_file(
                        current_len - len_at_start, config.HISTFILE
                    )
                    return
                except OSError:
                    # Most likely HISTFILE does not exist yet (first ever
                    # session). write_history_file() creates it.
                    pass
            readline.write_history_file(config.HISTFILE)

        if readline.get_current_history_length() == 0:
            # If no history was loaded, default to .python_history.
            # The guard is necessary to avoid doubling history size at
            # each interpreter exit when readline was already configured
            # see: http://bugs.python.org/issue5845#msg198636
            try:
                readline.read_history_file(config.HISTFILE)
            except OSError:
                pass
            len_at_start = readline.get_current_history_length()
            atexit.register(append_history, len_at_start)

        readline.set_history_length(config.HISTSIZE)

        # - 2. enable auto-indenting
        if config.AUTO_INDENT:
            readline.set_pre_input_hook(self.auto_indent_hook)

        # - 3. completion
        # - replace default completer
        self.completer = ImprovedCompleter(self.locals)
        readline.set_completer(self.completer.complete)

    def init_prompt(self, nested=False):
        """Activates color on the prompt based on python version.

        Also adds the hosts IP if running on a remote host over a
        ssh connection.
        """
        prompt_color = yellow
        sys.ps1 = prompt_color(">=> " if nested else ">>> ", readline_workaround=True)
        sys.ps2 = red("... ", readline_workaround=True)
        # - if we are over a remote connection, modify the ps1
        if os.getenv("SSH_CONNECTION"):
            ssh_parts = os.getenv("SSH_CONNECTION", "").split()
            this_host = ssh_parts[2] if len(ssh_parts) >= 3 else "remote"
            sys.ps1 = prompt_color(f"[{this_host}]>>> ", readline_workaround=True)
            sys.ps2 = red(f"[{this_host}]... ", readline_workaround=True)

    def init_pprint(self):
        """Activates pretty-printing of output values."""
        color_dict = partial(_RE_DICT_KEYS.sub, lambda m: purple(m.group()))

        def pprint_callback(value):
            # - like the default displayhook: a None result is neither
            # printed nor bound to '_', so '_' keeps the last real value
            if value is None:
                return
            # pprint pulls in dataclasses, so import it on first use
            # rather than at startup.
            import pprint

            # os.get_terminal_size() returns (columns, lines), so the
            # width is .columns. It raises OSError (not AttributeError)
            # when stdout is not a tty, eg. under a pipe.
            try:
                cols = os.get_terminal_size().columns
            except OSError:
                try:
                    cols = int(os.environ["COLUMNS"])
                except (KeyError, ValueError):
                    cols = 80
            formatted = pprint.pformat(
                value,
                width=cols,
                compact=True,
                sort_dicts=config.PPRINT_SORT_DICTS,
            )
            print(color_dict(formatted) if isinstance(value, dict) else blue(formatted))
            self.locals["_"] = value

        sys.displayhook = pprint_callback

    def _stop_asyncio_loop(self, announce=True):
        import ast

        self.loop.stop()
        self.locals.pop("repl_future", None)
        self.locals.pop("repl_future_interrupted", None)
        # Without a loop a top-level `await` can't run, so stop accepting
        # it. Otherwise the statement compiles to a coroutine that exec()
        # silently discards, and nothing in it runs - not even the code
        # before the await.
        self.compile.compiler.flags &= ~ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
        self.runcode = self.runcode_sync
        self.loop = None
        if announce:
            self.writeline(
                grey(
                    "Stopped the asyncio loop. Exit this nested REPL (Ctrl-D) "
                    "to return to the main prompt."
                )
            )

    def _init_nested_repl(self):
        import ast
        import asyncio
        import threading

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.compile.compiler.flags |= ast.PyCF_ALLOW_TOP_LEVEL_AWAIT
        self.locals["asyncio"] = asyncio
        self.locals["repl_future"] = None
        self.locals["repl_future_interrupted"] = False
        self.runcode = self.runcode_async

        def repl_thread():
            try:
                self.init_prompt(nested=True)
                self.interact(
                    banner=(
                        "An asyncio loop has been started in the main thread.\n"
                        "This nested interpreter is now running in a separate thread.\n"
                        f"Use {config.TOGGLE_ASYNCIO_LOOP_CMD} to stop the asyncio loop "
                        "and simply exit this nested interpreter to stop this thread\n"
                    ),
                    exitmsg="now exiting nested REPL...\n",
                )
            finally:
                import warnings

                warnings.filterwarnings(
                    "ignore",
                    message=r"^coroutine .* was never awaited$",
                    category=RuntimeWarning,
                )
                if self.loop and self.loop.is_running():
                    # - the user already left the nested REPL, so don't
                    # tell them to
                    self.loop.call_soon_threadsafe(
                        partial(self._stop_asyncio_loop, announce=False)
                    )

                self.init_prompt()

        self.repl_thread = threading.Thread(target=repl_thread)
        self.repl_thread.start()

    def _start_asyncio_loop(self):
        while self.loop is not None:
            try:
                self.loop.run_forever()
            except KeyboardInterrupt:
                if (
                    repl_future := self.locals.get("repl_future")
                ) and not repl_future.done():
                    repl_future.cancel()
                    self.locals["repl_future_interrupted"] = True

        # The loop is gone, but the nested REPL thread may still be reading
        # stdin (eg: the loop was stopped with the toggle command from
        # inside it). Wait for it to exit, otherwise this thread and that
        # one both read from the same stdin. Ctrl-C lands in this thread
        # while we wait and must not cut the wait short.
        while self.repl_thread.is_alive():
            try:
                self.repl_thread.join()
            except KeyboardInterrupt:
                pass

    @_doc_to_usage
    def toggle_asyncio(self, _):
        """{config.TOGGLE_ASYNCIO_LOOP_CMD} - Starts/stops the asyncio loop

        Configures the interpreter in a similar manner to `python -m asyncio`:
        an event loop runs in the main thread and a nested REPL, in which
        top-level `await` works, runs in a separate thread. Typing the
        command again inside the nested REPL stops the loop; exit the
        nested REPL (Ctrl-D) to get back to the main prompt.
        """
        import threading

        if self.loop is not None:
            if (
                repl_future := self.locals.get("repl_future")
            ) and not repl_future.done():
                repl_future.cancel()
            self.loop.call_soon_threadsafe(self._stop_asyncio_loop)
        elif threading.current_thread() is not threading.main_thread():
            # Starting a loop from inside the (now loop-less) nested REPL
            # would stack a third thread on top and leave the main thread
            # stuck waiting for this one.
            self.writeline(
                grey(
                    "The asyncio loop is stopped. Exit this nested REPL "
                    f"(Ctrl-D) before using {config.TOGGLE_ASYNCIO_LOOP_CMD} "
                    "again."
                )
            )
        else:
            self._init_nested_repl()
            self._start_asyncio_loop()

    def auto_indent_hook(self):
        """Hook called by readline between printing the prompt and
        starting to read input.
        """
        readline.insert_text(self._indent)
        readline.redisplay()

    @_doc_to_usage
    def toggle_auto_indent(self, _):
        """{config.TOGGLE_AUTO_INDENT_CMD} - Toggles the auto-indentation behavior"""
        hook = None if config.AUTO_INDENT else self.auto_indent_hook
        msg = "# Auto-Indent has been {}abled\n".format("en" if hook else "dis")
        config.AUTO_INDENT = bool(hook)

        if hook is None:
            msg += (
                "# End of blocks will be detected after 3 empty lines\n"
                f"# Re-type {config.TOGGLE_AUTO_INDENT_CMD} on a line by itself to enable"
            )

        readline.set_pre_input_hook(hook)
        print(grey(msg, bold=False))
        return ""

    def _cmd_handler(self, line):
        # - console commands and the doc suffix are only recognised at the
        # start of a statement. Inside a block the text is plain python
        # (eg: a docstring line ending with '?') and must go through as is.
        at_statement_start = not self.buffer
        if at_statement_start and (matches := self.commands_re.match(line)):
            command, args = matches.groups()
            line = self.commands[command](args)
        elif at_statement_start and line.endswith(config.DOC_CMD):
            if line.endswith(config.DOC_CMD * 2):
                # search for line in online docs
                # - strip off the '??' and the possible tab-completed
                # '(' or '.', turn inner '.' into spaces so each part is a
                # search term, and URL-encode the result
                import webbrowser
                from urllib.parse import quote_plus

                term = line.rstrip(f"{config.DOC_CMD}.(").replace(".", " ")
                webbrowser.open(config.DOC_URL.format(sys=sys, term=quote_plus(term)))
                line = ""
            else:
                line = line.rstrip(f"{config.DOC_CMD}.(")
                if not line:
                    line = "dir()"
                elif keyword.iskeyword(line):
                    line = f'help("{line}")'
                else:
                    line = f"print({line}.__doc__)"
        elif config.AUTO_INDENT and (
            line.startswith(config.ONE_INDENT) or self._indent
        ):
            if line.strip():
                # if non empty line with an indent, check if the indent
                # level has been changed
                leading_space = line[: line.index(line.lstrip()[0])]
                if self._indent != leading_space:
                    # indent level changed, update self._indent
                    self._indent = leading_space
            else:
                # - empty line, decrease indent
                self._indent = self._indent[: -len(config.ONE_INDENT)]
                line = self._indent
        elif at_statement_start and line.startswith("%"):
            self.writeline("Y U NO LIKE ME?")
            return line
        return line or ""

    def raw_input(self, prompt=""):
        """Read the input and delegate if necessary."""
        line = super().raw_input(prompt)
        empty_lines = 3 if line else 1
        while not config.AUTO_INDENT and empty_lines < 3:
            line = super().raw_input(prompt)
            empty_lines += 1 if not line else 3
        return self._cmd_handler(line)

    def push(self, line, *args, **kwargs):
        """Wrapper around InteractiveConsole's push method for adding an
        indent on start of a block.
        """
        if more := super().push(line, *args, **kwargs):
            if line.endswith((":", "[", "{", "(")):
                self._indent += config.ONE_INDENT
        else:
            self._indent = ""
        return more

    def runcode_async(self, code):
        import asyncio
        import concurrent.futures
        import inspect
        from types import FunctionType

        future = concurrent.futures.Future()

        def callback():
            self.locals["repl_future"] = None
            self.locals["repl_future_interrupted"] = False

            func = FunctionType(code, self.locals)
            try:
                coro = func()
            except SystemExit:
                raise
            except BaseException as ex:
                if isinstance(ex, KeyboardInterrupt):
                    self.locals["repl_future_interrupted"] = True
                future.set_exception(ex)
                return

            if not inspect.iscoroutine(coro):
                future.set_result(coro)
                return

            try:
                self.locals["repl_future"] = self.loop.create_task(coro)
                asyncio.futures._chain_future(self.locals["repl_future"], future)
            except BaseException as exc:
                future.set_exception(exc)

        self.loop.call_soon_threadsafe(callback)

        try:
            return future.result()
        except SystemExit:
            raise
        except NameError as err:
            # - same auto-import as the synchronous path. The innermost
            # traceback frame is still the statement's own code object,
            # whether it ran as a plain function or as a coroutine.
            if config.ENABLE_AUTO_IMPORTS and self._auto_import(err, code):
                return self.runcode(code)
            self.showtraceback()
        except BaseException:
            if self.locals.get("repl_future_interrupted"):
                self.write("\nKeyboardInterrupt\n")
            else:
                self.showtraceback()

    def write(self, data):
        """Write out data to stderr"""
        data = str(data)
        sys.stderr.write(data if data.startswith("\033[") else red(data))

    def writeline(self, data):
        """Same as write but adds a newline to the end"""
        return self.write(f"{data}\n")

    def resetbuffer(self):
        self._indent = previous = ""
        for line in self.buffer:
            # - replace multiple empty lines with one before writing to session history
            stripped = line.strip()
            if stripped or stripped != previous:
                self.session_history.append(line)
            previous = stripped
        return super().resetbuffer()

    def _mktemp_buffer(self, lines):
        """Writes lines to a temp file and returns the filename."""
        from tempfile import NamedTemporaryFile

        with NamedTemporaryFile(mode="w+", suffix=".py", delete=False) as tempbuf:
            tempbuf.write("\n".join(lines))
        return tempbuf.name

    def showtraceback(self, *args, **kwargs):
        """Wrapper around super(..).showtraceback()

        We do this to detect whether any subsequent statements after a
        traceback occurs should be skipped. This is relevant when
        executing multiple statements from an edited buffer.
        """
        self._skip_subsequent = True
        return super().showtraceback(*args, **kwargs)

    def showsyntaxerror(self, *args, **kwargs):
        """A syntax error must stop an edited buffer just like a traceback."""
        self._skip_subsequent = True
        return super().showsyntaxerror(*args, **kwargs)

    def _exec_from_file(
        self, open_fd, quiet=False, skip_history=False, print_comments=None
    ):
        if print_comments is None:
            # - resolved at call time so a .venv_rc.py can change the config
            print_comments = config.POST_EDIT_PRINT_COMMENTS
        self._skip_subsequent = False
        previous = ""
        for stmt in open_fd:
            # - skip over multiple empty lines
            stripped = stmt.strip()
            if stripped == previous == "":
                continue

            # - if line is a comment, print (if required) and move to
            # next line
            if stripped.startswith("#"):
                if print_comments and not quiet:
                    self.write(grey(f"... {stmt}", bold=False))
                continue

            # - process line only if we haven't encountered an error yet
            if not self._skip_subsequent:
                line = stmt.strip("\n")
                if line and not line[0].isspace():
                    # - end of previous statement, submit buffer for
                    # execution
                    source = "\n".join(self.buffer)
                    more = self.runsource(source, self.filename)
                    if not more:
                        self.resetbuffer()

            if not quiet:
                self.write(cyan(f"... {stmt}", bold=(not self._skip_subsequent)))

            if self._skip_subsequent:
                self.session_history.append(stmt)
            else:
                self.buffer.append(line)
                if not skip_history:
                    readline.add_history(line)
            previous = stripped
        self.push("")

    def lookup(self, name: str, namespace=None):
        """Look up a (dotted) name in *namespace*, or in the current locals
        with a fallback to builtins (so `len` or `str.join` resolve too).

        Iterative rather than recursive so arbitrarily deep dotted names
        (e.g. a.b.c.d.e) don't risk hitting Python's recursion limit.
        """
        import builtins

        parts = name.split(".")
        if namespace is None:
            obj = self.locals.get(parts[0], getattr(builtins, parts[0], None))
        else:
            obj = getattr(namespace, parts[0], None)
        for part in parts[1:]:
            if obj is None:
                return None
            obj = getattr(obj, part, None)
        return obj

    @_doc_to_usage
    def process_edit_cmd(self, arg=""):
        """{config.EDIT_CMD} [object|filename]

        Open {config.EDITOR} with session history, provided filename or
        object's source file.

        - without arguments, a temporary file containing session history is
          created and opened in {config.EDITOR}. On quitting the editor, all
          the non commented lines in the file are executed, if the
          editor exits with a 0 return code (eg: if editor is `vim`, and
          you exit using `:cq`, nothing from the buffer is executed and
          you are returned to the prompt).

        - with a filename argument, the file is opened in the editor. On
          close, you are returned back to the interpreter.

        - with an object name argument, an attempt is made to lookup the
          source file of the object and it is opened if found. Else the
          argument is treated as a filename.
        """
        import inspect
        import shlex
        import subprocess

        line_num_opt = ""
        tempbuf = None
        if arg:
            try:
                if (obj := self.lookup(arg)) is not None:
                    filename = inspect.getsourcefile(obj)
                    if filename is None:
                        return self.writeline(f"No source file available for {arg}")
                    _, line_no = inspect.getsourcelines(obj)
                    line_num_opt = config.LINE_NUM_OPT.format(line_no=line_no)
                else:
                    filename = arg
            except (OSError, TypeError, NameError) as e:
                return self.writeline(e)
        else:
            # - make a list of all lines in history, commenting any non-blank lines.
            if not (history := self.session_history):
                # - brand new session: offer the saved history instead
                try:
                    with open(config.HISTFILE) as hf:
                        history = [
                            line
                            for line in hf
                            # libedit's history file starts with a marker
                            if not line.startswith("_HiStOrY_V2_")
                        ]
                except OSError:
                    history = []
            filename = tempbuf = self._mktemp_buffer(
                f"# {line}" if line.strip() else ""
                for line in (line.strip("\n") for line in history)
            )
            line_num_opt = config.LINE_NUM_OPT.format(line_no=len(history))

        # - shell out to the editor. Only EDITOR is shell-split (it may carry
        # its own flags); the filename is passed through untouched so paths
        # containing spaces or quotes still work.
        editor_argv = shlex.split(config.EDITOR)
        if line_num_opt:
            editor_argv.append(line_num_opt)
        editor_argv.append(filename)
        try:
            try:
                rc = subprocess.run(editor_argv, check=False).returncode
            except OSError as e:
                return self.writeline(f"Could not run {config.EDITOR}: {e}")

            # - if arg was not provided (ie: we edited history), execute
            # un-commented lines in the current namespace
            if tempbuf is None:
                return None
            if rc != 0:
                return self.writeline(
                    f"{config.EDITOR} exited with an error code. Skipping execution."
                )
            # - if HISTFILE contents were edited (ie: EDIT_CMD in a brand
            # new session), don't print commented out lines
            print_comments = (
                config.POST_EDIT_PRINT_COMMENTS
                if history is self.session_history
                else False
            )
            with open(tempbuf) as edits:
                self._exec_from_file(edits, print_comments=print_comments)
        finally:
            if tempbuf is not None:
                os.unlink(tempbuf)
        return None

    @_doc_to_usage
    def process_sh_cmd(self, cmd):
        """{config.SH_EXEC} [cmd [args ...] | {{fmt string}}]

        Escape to {config.SHELL} or execute `cmd` in {config.SHELL}

        - without arguments, the current interpreter will be suspended
          and you will be dropped in a {config.SHELL} prompt. Use fg to return.

        - with arguments, the text is run by `{config.SHELL} -c`, so globs,
          pipes and redirections work. Its stdout is displayed in green
          (red if the command failed), its stderr in red, and '_' will
          contain the CompletedProcess (.stdout, .stderr, .returncode).

        - {config.SH_EXEC}{config.SH_EXEC} cmd runs the command interactively
          on the terminal instead of capturing its output. Use it for
          editors, pagers, `git log` and anything else that needs the tty.

        - `cd [dir]` and `cd -` change the interpreter's working directory.

          You may pass values from the namespace to the command line using
          the `.format()` syntax. Only `{{name}}` and `{{dotted.name}}` that
          resolve to something in the namespace are expanded; any other
          braces (eg: awk '{{print $1}}' or find -exec rm {{}} \\;) reach
          the shell untouched. For example:

        >>> filename = '/does/not/exist'
        >>> !ls {{filename}}
        ls: /does/not/exist: No such file or directory
        >>> _.returncode
        1
        """
        import shlex
        import signal
        import subprocess

        if not cmd:
            if os.getenv("SSH_CONNECTION"):
                # I use the bash function similar to the one below in my
                # .bashrc to directly open a python prompt on remote
                # systems I log on to.
                #   function rpython { ssh -t $1 -- "python" }
                # Unfortunately, suspending this ssh session, does not place me
                # in a shell, so I need to create one:
                os.system(config.SHELL)
            elif not sys.stdin.isatty():
                # - no job control to `fg` us from: SIGSTOP would freeze the
                # process for good
                self.writeline("Not attached to a terminal, cannot suspend.")
            else:
                os.kill(os.getpgrp(), signal.SIGSTOP)
            return

        interactive = cmd.startswith(config.SH_EXEC)
        if interactive:
            cmd = cmd[len(config.SH_EXEC) :].strip()
        try:
            cmd = self._expand_shell_fields(cmd)
            try:
                argv = shlex.split(cmd)
            except ValueError:
                argv = []  # - unbalanced quotes: let the shell report it
            if argv and argv[0] == "cd":
                self._chdir(" ".join(argv[1:]))
            elif interactive:
                self.locals["_"] = subprocess.run(
                    [config.SHELL, "-c", cmd], check=False
                )
            else:
                completed = subprocess.run(
                    [config.SHELL, "-c", cmd],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if out := completed.stdout:
                    if not out.endswith("\n"):
                        out += "\n"
                    sys.stdout.write(
                        red(out) if completed.returncode else green(out, bold=False)
                    )
                if err := completed.stderr:
                    if not err.endswith("\n"):
                        err += "\n"
                    sys.stderr.write(red(err))
                self.locals["_"] = completed
        except Exception:
            self.showtraceback()

    def _expand_shell_fields(self, cmd):
        """Expand {name} / {dotted.name} from the namespace, leave the rest."""

        def expand(match):
            field = match.group(1)
            # - only names bound in the namespace, not builtins: the braces
            # in awk '{print}' must stay exactly as typed
            if field.partition(".")[0] not in self.locals:
                return match.group(0)
            value = self.lookup(field)
            return match.group(0) if value is None else str(value)

        return _RE_SHELL_FIELD.sub(expand, cmd)

    def _chdir(self, target):
        """`cd` for the shell command, including `cd -` like a real shell."""
        if target == "-":
            target = os.environ.get("OLDPWD")
            if not target:
                return self.writeline("cd: OLDPWD not set")
            print(target)
        target = os.path.expanduser(os.path.expandvars(target or "~"))
        previous = os.getcwd()
        os.chdir(target)
        os.environ["OLDPWD"] = previous
        os.environ["PWD"] = os.getcwd()
        return None

    @_doc_to_usage
    def process_list_cmd(self, arg):
        """{config.LIST_CMD} <object> - List source code for object, if possible."""
        import inspect

        if not arg:
            return self.writeline(
                f"source list command requires an argument (eg: {config.LIST_CMD} foo)"
            )
        obj = self.lookup(arg)
        if obj is None:
            return self.writeline(f"{arg}: no such name in the namespace")
        try:
            src_lines, offset = inspect.getsourcelines(obj)
        except TypeError:
            # - builtins and other C-level objects have no python source
            self.writeline(
                f"{arg}: no python source available ({type(obj).__name__} object)"
            )
        except OSError as e:
            self.writeline(e)
        else:
            for line_no, line in enumerate(src_lines, offset + 1):
                self.write(cyan(f"{line_no:03d}: {line}"))

    @_doc_to_usage
    def process_help_cmd(self, arg):
        """{config.HELP_CMD} [keyword|object|command]

        Print help for a keyword, an object or one of this console's commands.

        - without arguments, print the feature list for this console.
        - with a python keyword or any object, print its help/docstring.
        - with one of the console commands, print that command's usage.
        """
        if not arg:
            print(cyan(self.__doc__).format(**config.__dict__))
            return None
        if arg in self.commands:
            return self.commands[arg]("-h")
        # - the returned line is executed by the console like typed input
        if keyword.iskeyword(arg):
            return f'help("{arg}")'
        return f"help({arg})"

    def exec_venv_rc(self):
        """Execute the venv rc file from the current directory, once.

        Returns the status line for the banner. Guarded so that a nested
        REPL (see toggle_asyncio) neither re-runs the file nor wipes the
        session history a second time.
        """
        if self._venv_rc_status is None:
            try:
                with open(config.VENV_RC) as venv_rc:
                    self._exec_from_file(venv_rc, quiet=True, skip_history=True)
            except OSError:
                self._venv_rc_status = cyan("(no venv rc found)")
            else:
                # - clear out session_history for venv_rc commands
                self.session_history = []
                self._venv_rc_status = green("Successfully executed venv rc !")
        return self._venv_rc_status

    def interact(self, banner=None, exitmsg=None):
        """A forgiving wrapper around InteractiveConsole.interact()"""
        venv_rc_done = self.exec_venv_rc()

        if banner is None:
            banner = (
                f"Welcome to the ImprovedConsole (version {__version__})\n"
                f"Type in {config.HELP_CMD} for list of features.\n"
                f"{venv_rc_done}"
            )

        retries = 2
        while retries:
            try:
                super().interact(banner=banner, exitmsg=exitmsg)
            except SystemExit:
                # Fixes #2: exit when 'quit()' invoked
                break
            except Exception:
                import traceback

                retries -= 1
                print(
                    red(
                        "I'm sorry, ImprovedConsole could not handle that !\n"
                        "Please report an error with this traceback, "
                        "I would really appreciate that !"
                    )
                )
                traceback.print_exc()

                print(
                    red(
                        "I shall try to restore the crashed session.\n"
                        "If the crash occurs again, please exit the session"
                    )
                )
                # - drop the statement that took us down, or it would be
                # glued to the next line typed into the restored session
                self.resetbuffer()
                banner = blue("Your crashed session has been restored")
            else:
                # exit with a Ctrl-D
                break

        # Exit the Python shell on exiting the InteractiveConsole
        import threading

        if threading.current_thread() == threading.main_thread():
            sys.exit()


if not os.getenv("SKIP_PYMP"):
    # - create our pimped out console and fire it up !
    pymp = ImprovedConsole(locals=CLEAN_NS)
    CLEAN_NS["__pymp__"] = pymp
    pymp.interact()
