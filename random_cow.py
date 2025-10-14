#!/usr/bin/python3 -O

from random import shuffle
from subprocess import PIPE, Popen, run
from sys import exit as sysexit

# Path to cowsay executable
COWSAY: str = "/usr/games/cowsay"


def get_cows() -> list[str]:
    try:
        results: str = (
            run([COWSAY, "-l"], capture_output=True)
            .stdout.decode("utf-8")
            .split(":")[1]
        )
        cows: list[str] = []
        for result in results.split():
            cows.append(result.strip())
        shuffle(cows)
    except Exception as ex:
        print(f"Unable to get cow list: {ex}")
        sysexit(1)
    return cows


def get_fortune() -> str:
    try:
        fortune: str = run(
            ["/usr/games/fortune", "-a"], capture_output=True
        ).stdout.decode("utf-8")
        return fortune
    except Exception as ex:
        print(f"Unable to get fortune: {ex}")
        sysexit(1)


def print_fortune(fortune: str, cowlist: list[str]) -> None:
    try:
        cow: str = cowlist[0]
        cow_pipe: Popen = Popen(
            ["/usr/games/cowsay", "-f", cow],
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            text=True,
        )
        cow_fortune: str = cow_pipe.communicate(input=fortune)[0]
        print(cow_fortune)
    except Exception as ex:
        print(f"Unable to print fortune: {ex}")
        sysexit(1)


if __name__ == "__main__":
    cowlist: list[str] = get_cows()
    fortune: str = get_fortune()
    print_fortune(fortune, cowlist)
