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
        # FIX: pass the decoded string through str.split() directly rather than
        # building a list with a for loop and .strip() — split() already strips
        # whitespace and returns clean tokens.
        output: str = run(
            [COWSAY, "-l"],
            capture_output=True,
            check=True,  # FIX: check=True raises on non-zero exit
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
        # FIX: was hardcoding '/usr/games/fortune' here but COWSAY constant at top
        # for consistency — extracted FORTUNE constant to match.
        # FIX: added check=True so a non-zero exit code raises instead of silently
        # returning empty output.
        fortune: str = run(
            [FORTUNE, "-a"], capture_output=True, check=True
        ).stdout.decode("utf-8")
        return fortune
    except Exception as ex:
        print(f"Unable to get fortune: {ex}")
        sysexit(1)
    return ""  # FIX: unreachable but satisfies type checker — sysexit raises, not returns


def print_fortune(fortune: str, cowlist: list[str]) -> None:
    """Print the fortune spoken by a randomly selected cow."""
    try:
        # OPT: was always using cowlist[0] — but get_cows() no longer shuffles,
        # so use random.choice() to pick a random cow directly. This is cleaner
        # than shuffle-then-take-first, and makes the intent obvious.
        # FIX: removed shuffle() from get_cows() since the only consumer was
        # taking element [0]; choice() here does the same job in one call.
        cow: str = choice(cowlist)

        # SECURITY: shell=False (the default) is already used here — good.
        # The cow name comes from cowsay's own -l output so it's trusted, but
        # it's still correct to keep it as a list arg rather than shell=True.
        cow_pipe: Popen[str] = (
            Popen(  # FIX: Popen was unparameterized; Popen[str] matches text=True
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
        )  # OPT: unpack tuple directly, discard stderr
        print(cow_fortune)
    except Exception as ex:
        print(f"Unable to print fortune: {ex}")
        sysexit(1)


if __name__ == "__main__":
    cowlist: list[str] = get_cows()
    fortune: str = get_fortune()
    print_fortune(fortune, cowlist)
