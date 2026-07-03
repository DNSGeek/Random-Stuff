# Random-Stuff

> A curated grab-bag of Python (and a little C) apps, scripts, and libraries
> collected over the years — the useful, the interesting, and the just-plain-fun.

[![Language: Python](https://img.shields.io/badge/language-Python%203-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Language: C](https://img.shields.io/badge/language-C-A8B9CC?logo=c&logoColor=black)](#retro--fun)
[![License: GPL v2](https://img.shields.io/badge/license-GPL%20v2-blue.svg)](LICENSE.md)
[![Security Policy](https://img.shields.io/badge/security-policy-brightgreen.svg)](SECURITY.md)

Nothing here shares a single theme — that's the point. Each item is a
self-contained tool or library that solved a real problem (or scratched a real
itch). Browse the categories below, grab what looks useful, and ignore the rest.

---

## Table of Contents

- [Highlights](#highlights)
- [System Monitoring & Stats](#system-monitoring--stats)
- [Networking & Distributed Computing](#networking--distributed-computing)
- [Security & Cryptography](#security--cryptography)
- [LLM-Powered Tools](#llm-powered-tools)
- [Web App Example](#web-app-example)
- [Retro & Fun](#retro--fun)
- [Shared Libraries & Dotfiles](#shared-libraries--dotfiles)
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)

---

## Highlights

If you only look at a few things, make it these:

- 🔐 **[`cookieMonster.py`](cookieMonster.py)** — securely encrypt/decrypt data
  inside HTTP header cookies using Fernet, with the key interleaved among the
  payload segments.
- 📦 **[`tcpQueue.py`](tcpQueue.py)** — a bidirectional TCP message queue with
  on-disk SQLite (WAL) durability and optional HMAC-SHA256 authentication.
- ❤️ **[`heartbeat.py`](heartbeat.py)** — a deterministic (no split-brain)
  primary/secondary failover daemon for two-node clusters.
- 🤖 **[`summarize_git.py`](summarize_git.py)** — point a local LLM at a repo and
  get back a concise, human-readable summary.

---

## System Monitoring & Stats

Tools for collecting, exposing, and visualizing host metrics.

| Script                                     | What it does                                                                                                         |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| [`linux_sys_stats.py`](linux_sys_stats.py) | Pulls relevant system stats on Linux (via `top`) and prints them.                                                    |
| [`mac_sys_stats.py`](mac_sys_stats.py)     | The macOS counterpart — same idea, tuned for `top -l 1`.                                                             |
| [`sys_stats_api.py`](sys_stats_api.py)     | A tiny Flask REST wrapper that serves the `*_sys_stats.py` output as JSON (auto-selects Linux/macOS at runtime).     |
| [`web_stats.py`](web_stats.py)             | Polls multiple hosts, consolidates their stats, and renders graphs with matplotlib.                                  |
| [`checkmedia.py`](checkmedia.py)           | Recursively hashes every file in a directory (multithreaded, SQLite-backed) to detect if anything has been modified. |

**Quick look — serve stats as JSON:**

```bash
pip install flask
python3 sys_stats_api.py   # then GET the endpoint it exposes
```

---

## Networking & Distributed Computing

| Script                         | What it does                                                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`tcpQueue.py`](tcpQueue.py)   | Durable, crash-safe bidirectional message queue over TCP. Frames are length-prefixed with per-message opcodes and optional HMAC-SHA256 auth; queues persist to a WAL-mode SQLite DB. |
| [`heartbeat.py`](heartbeat.py) | Two-node active/standby failover daemon. Uses a deterministic `(ip, port)` election rule to avoid split-brain, with a state-change callback hook for VIP/service takeover.           |
| [`quova.py`](quova.py)         | An early Python 3 emulator of the Quova GeoIP protocol, backed by the MaxMind GeoLiteCity database.                                                                                  |

---

## Security & Cryptography

| Script                                 | What it does                                                                                                                                              |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`cookieMonster.py`](cookieMonster.py) | Encrypts/decrypts arbitrary data for transport inside HTTP cookies using Fernet, splitting and interleaving the key with the ciphertext (base85-encoded). |
| [`genpw.py`](genpw.py)                 | Generates memorable passphrases by combining multiple random words.                                                                                       |
| [`checkmedia.py`](checkmedia.py)       | File-integrity monitor — detects tampering by comparing content hashes against a saved baseline.                                                          |

> ⚠️ These tools are provided as-is. Review the code and the [Security Policy](SECURITY.md)
> before relying on them for anything sensitive.

---

## LLM-Powered Tools

Both talk to a local, OpenAI-compatible endpoint (e.g. [LM Studio](https://lmstudio.ai/)
at `http://127.0.0.1:1234`) — no data leaves your machine.

| Script                                         | What it does                                                                                                                                          |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`summarize_git.py`](summarize_git.py)         | Uses a local LLM to generate a concise summary of a Git repository.                                                                                   |
| [`summarize_readmes.py`](summarize_readmes.py) | Walks subdirectories, feeds their READMEs to a local LLM, and produces a combined summary — sizing input to the model's context window automatically. |

---

## Web App Example

[`PyWebApp/`](PyWebApp/) is a small, self-contained web application demonstrating
[CherryPy](https://cherrypy.dev/) + [Django](https://www.djangoproject.com/)
templating together with the `cookieMonster` encrypted-cookie library for secure
session data. It ships with HTML templates and a helper script to generate a
self-signed SSL certificate.

```bash
cd PyWebApp
pip install -r requirements.txt
python3 index.py            # serves on port 8080 by default
```

---

## Retro & Fun

| Item                             | What it does                                                                                                                                                                        |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`paravia.c`](paravia.c)         | A C implementation of the classic _Santa Paravia en Fiumaccio_ medieval city-management game.                                                                                       |
| [`pong.py`](pong.py)             | A [MicroPython](https://micropython.org/) badge app for the [Pimoroni Tufty 2040](https://shop.pimoroni.com/products/tufty-2040) — displays your name and an animated game of Pong. |
| [`random_cow.py`](random_cow.py) | Picks a random `cowsay` cow, grabs a `fortune`, and prints the two together.                                                                                                        |
| [`vushta.py`](vushta.py)         | Generates a random "Forbidden Delight" — an homage to the pleasures of the City of Vushta.                                                                                          |

---

## Shared Libraries & Dotfiles

| Item                         | What it does                                                                                                                                                                                             |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`myfuncs.py`](myfuncs.py)   | A grab-bag of small utility helpers (timing decorators, timeouts, etc.) reused across the other scripts.                                                                                                 |
| [`pythonrc.py`](pythonrc.py) | An enhanced, optimized Python interactive-shell startup file — colored prompts, history, tab completion, source listing, and more. Based on [lonetwin's pythonrc](https://github.com/lonetwin/pythonrc). |

**Use the enhanced interactive shell:**

```bash
export PYTHONSTARTUP=/path/to/pythonrc.py
python3
```

---

## Getting Started

Everything here targets **Python 3** (the C and MicroPython items are the
exceptions). There's no single package to install — each script is standalone,
so just grab the one you want and install its dependencies.

```bash
git clone https://github.com/DNSGeek/Random-Stuff.git
cd Random-Stuff
```

Common third-party dependencies, by tool:

| Dependency                 | Used by                            |
| -------------------------- | ---------------------------------- |
| `cryptography`             | `cookieMonster.py`, `PyWebApp/`    |
| `flask`                    | `sys_stats_api.py`                 |
| `matplotlib`, `requests`   | `web_stats.py`, `summarize_git.py` |
| `cherrypy`, `django`       | `PyWebApp/`                        |
| `python-daemon`, `pygeoip` | `quova.py`                         |

Install what a given tool needs, e.g.:

```bash
pip install cryptography      # for cookieMonster.py
```

Most scripts print usage with `-h`/`--help` or have configuration constants
documented near the top of the file.

---

## Contributing

Issues and pull requests are welcome. Because these are independent tools,
please keep a change scoped to a single script where possible, and match the
existing style (type hints and docstrings are used throughout the Python code).

---

## Security

Found a vulnerability? Please report it responsibly — see [SECURITY.md](SECURITY.md)
for the disclosure process. Don't open a public issue for security reports.

---

## License

This project is licensed under the **GNU General Public License v2** — see
[LICENSE.md](LICENSE.md) for the full text. (`pythonrc.py` retains its original
MIT license from the upstream project.)
