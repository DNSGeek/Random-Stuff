#!/usr/bin/python3 -uO

import argparse
from random import shuffle
from typing import List

# I used the file google-10000-english-usa-no-swears.txt
# from the github repo at
# https://github.com/first20hours/google-10000-english
# Feel free to use the word file of your choice.
def readWords(filename: str) -> List[str]:
    retlist: List[str] = []
    with open(filename, "rt") as wl:
        for word in wl.readlines():
            retlist.append(word.strip())
    return retlist


def genPW(wordlist: List[str], numwords: int):
    shuffle(wordlist)
    print(" ".join(wordlist[:numwords]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--wordfile",
        help="The word file to read",
        default="words.txt",
        required=False,
        )
    parser.add_argument(
        "-n",
        "--numwords",
        type=int,
        help="The number of words to generate",
        default=4,
        required=False,
    )
    args = parser.parse_args()
    wordlist = readWords(args.wordfile)
    genPW(wordlist, args.numwords)
