#!/usr/bin/python3 -O

from random import choice
from subprocess import PIPE, Popen, run
from sys import exit as sysexit

# Paths to external executables
COWSAY: str = "/usr/games/cowsay"
FORTUNE: str = "/usr/games/fortune"


def get_cows() -> list[str]:
    """Return a list of available cowsay cow names, in random order."""
    try:
        output: str = run(
            [COWSAY, "-l"],
            capture_output=True,
            check=True,
        ).stdout.decode("utf-8")
        # The first line is a header ending with ':', cows follow after the colon.
        cows: list[str] = output.split(":", 1)[1].split()
    except Exception as ex:
        print(f"Unable to get cow list: {ex}")
        sysexit(1)
    return cows


def get_fortune() -> str:
    """Return a random fortune string."""
    try:
        fortune: str = run(
            [FORTUNE, "-a"], capture_output=True, check=True
        ).stdout.decode("utf-8")
        return fortune
    except Exception as ex:
        print(f"Unable to get fortune: {ex}")
        sysexit(1)
    return ""


def print_fortune(fortune: str, cowlist: list[str]) -> None:
    """Print the fortune spoken by a randomly selected cow."""
    try:
        cow: str = choice(cowlist)

        cow_pipe: Popen[str] = (
            Popen(
                [COWSAY, "-f", cow],
                stdin=PIPE,
                stdout=PIPE,
                stderr=PIPE,
                text=True,
            )
        )
        cow_fortune: str
        cow_fortune, _ = cow_pipe.communicate(
            input=fortune
        )
        print(cow_fortune)
    except Exception as ex:
        print(f"Unable to print fortune: {ex}")
        sysexit(1)


if __name__ == "__main__":
    cowlist: list[str] = get_cows()
    fortune: str = get_fortune()
    print_fortune(fortune, cowlist)
