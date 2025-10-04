#!/usr/bin/python3

import argparse
import os
from typing import Union

import requests


def getDirs(base_dir: str = ".") -> list[str]:
    dirs: list[str] = []
    for name in os.listdir(base_dir):
        dname = os.path.join(base_dir, name)
        if os.path.isdir(dname):
            if name.startswith("."):
                continue
            dirs.append(name)
    return dirs


def pullReadme(basename: str, dirname: str) -> str:
    dirname = os.path.join(basename, dirname)
    files = os.listdir(dirname)
    for file in files:
        if file.lower() == "readme.md":
            file = os.path.join(dirname, file)
            with open(file, "rt") as readme:
                lines = readme.readlines()
            text = "".join(lines)
            return text
    return ""


def pullAI(text: str, session: requests.Session) -> str:
    headers: dict[str, str] = {}
    headers["Content-Type"] = "application/json"
    data: dict[str, Union[str, int, float, list[dict[str, str]]]] = {}
    data["model"] = "openai/gpt-oss-20b"
    data["temperature"] = 0.7
    data["max_tokens"] = -1
    data["stream"] = False
    data["messages"] = []
    data["messages"].append(
        {
            "role": "system",
            "content": "You are friendly and helpful. You will answer all requests to the best of your ability.",
        }
    )
    data["messages"].append(
        {
            "role": "user",
            "content": "Please write a 2 sentence summary of the following document.\n\n"
            + text,
        }
    )
    try:
        response = session.post(
            "http://127.0.0.1:1234/v1/chat/completions",
            headers=headers,
            json=data,
        )
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        response.close()
        return content
    except:  # Yeah, yeah. I know. I don't care what the error was thoiugh.
        return ""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--directory", help="The base directory to scan", required=True
    )
    args = parser.parse_args()
    summaries: dict[str, str] = {}
    failed: list[str] = []
    session: requests.Session = requests.Session()
    basedir: str = args.directory
    currdir: list[str] = getDirs(basedir)
    for checkdir in currdir:
        text = pullReadme(basedir, checkdir)
        if text:
            response = pullAI(text, session)
            if response:
                summaries[checkdir] = response
                print(f"{checkdir}: {summaries[checkdir]}\n")
            else:
                failed.append(checkdir)
    with open("summaries.txt", "wt") as summ:
        for key in sorted(summaries.keys()):
            summ.write(f"{key}: {summaries[key]}\n\n")
    print(f"Failed: {sorted(failed)}")
