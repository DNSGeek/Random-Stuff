#!/usr/bin/env python3 -uO

from random import randint, seed, shuffle
from typing import Optional


class ForbiddenDelight(object):
    """The class of all Forbidden Delights."""

    def __init__(self) -> None:
        """Prepare a new Forbidden Delight for the discerning customer."""
        self.beasts: list[str] = []
        self.beasts.append("bog wombler")
        self.beasts.append("rok")
        self.beasts.append("griffen")
        self.beasts.append("centaur")
        self.beasts.append("hippogriff")
        self.beasts.append("tiny Brownie")
        self.beasts.append("fairy")
        self.beasts.append("gay unicorn")
        self.beasts.append("moderately worshipful Plaugg")
        self.beasts.append("grackle")
        self.beasts.append("werechicken")
        self.beasts.append("wererabbit")
        self.beasts.append("golden pooping chicken")
        self.beasts.append("demon")
        self.beasts.append("dread collector")
        self.beasts.append("troll")
        self.beasts.append("vengeful witch")
        self.beasts.append("sneezing wizard")
        self.beasts.append("dealer of death")
        self.beasts.append("truth telling demon")
        self.beasts.append("million butterflies")
        self.beasts.append("3 day old dead haddock")
        self.beasts.append("very fat warrior")
        self.beasts.append("dancing dragon")
        self.beasts.append("singing damsel")
        self.beasts.append("clumsy apprentice")
        self.beasts.append("union giant")
        self.beasts.append("harpy")
        self.beasts.append("satyr")
        self.beasts.append("chimera")
        self.beasts.append("kelpie")
        self.beasts.append("nixie")
        self.beasts.append("pooka")
        self.beasts.append("sphinx")
        self.beasts.append("nymph")
        self.beasts.append("400 year old ghostly comedian")
        self.beasts.append("Death")
        self.beasts.append("bowling champion")
        self.beasts.append("talking wolf")
        self.beasts.append("declaiming demon")
        self.beasts.append("tour bus driver")
        self.beasts.append("clumsy giant")
        self.beasts.append("ferret")

        self.things: list[str] = []
        self.things.append("a giant brownie shoe")
        self.things.append("a bog")
        self.things.append("an oak staff")
        self.things.append("a werestone")
        self.things.append("some seed corn")
        self.things.append("a rented warclub")
        self.things.append("pastries")
        self.things.append("a large vat of lemon custard")
        self.things.append("the happy woodcutter song")
        self.things.append("a tone deaf singing sword")
        self.things.append("a winged helmet")
        self.things.append("a vaudeville act")
        self.things.append("a netherhells incursion")
        self.things.append("a religious musical")
        self.things.append("Mother Duck")
        self.things.append("seven other dwarves")
        self.things.append("giant bread ovens")
        self.things.append("Cuthbert, the cowardly sword")
        self.things.append("a hermit retreat")
        self.things.append("Wonk, the horn of persuasion")
        self.things.append("a get out of jail free card")
        self.things.append("a slime works")
        self.things.append("a slime burger")
        self.things.append("a ripped magicians hat")

    def Delight(self, svalue: Optional[int] = None) -> str:
        """Return a random Forbidden Delight"""
        seed(svalue)
        shuffle(self.beasts)
        shuffle(self.things)
        return (
            "Your forbidden delight -- for only %d gold coins -- involves a %s, %s and a %s."
            % (
                randint(100, 1000),
                self.beasts[0],
                self.things[0],
                self.beasts[1],
            )
        )


if __name__ == "__main__":
    foo: ForbiddenDelight = ForbiddenDelight()
    print(foo.Delight())
