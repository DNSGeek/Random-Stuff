import random
import time

from machine import ADC, Pin
from picographics import DISPLAY_TUFTY_2040, PicoGraphics

display = PicoGraphics(display=DISPLAY_TUFTY_2040)

# Change your First and Last name in the function
# displayScore and fix the position to make them centered.


class Player:
    def __init__(self, x):
        self.xpos = x
        self.ypos = 130
        self.height = 50
        self.up = True if random.random() <= 0.5 else False
        self.count = 0
        self.score = 0
        self.ybottom = self.ypos + self.height
        self.smooth = 15

    def reset(self):
        self.ypos = 130
        self.up = True if random.random() <= 0.5 else False
        self.count = 0
        self.score = 0
        self.ybottom = self.ypos + self.height

    def moveUp(self):
        if self.ypos > 92:
            self.ypos -= 1
        else:
            self.up = False
        self.ybottom = self.ypos + self.height

    def moveDown(self):
        if self.ypos < 190:
            self.ypos += 1
        else:
            self.up = True
        self.ybottom = self.ypos + self.height

    def collision(self, bally):
        if bally >= self.ypos and bally <= self.ybottom:
            return True
        return False

    def move(self):
        if self.count < self.smooth:
            self.count += 1
            if self.up:
                self.moveUp()
            else:
                self.moveDown()
            return
        self.count = 0
        self.up = True if random.random() <= 0.5 else False

    def addScore(self):
        self.score += 1

    def getScore(self):
        return self.score

    def getPosition(self):
        return self.ypos


def getBacklightLevel(l):
    # Keep the display dim to save battery
    reading = float(l.read_u16())
    # Values seem to be between 0.0 - 25000.0
    bl = reading / 25000.0
    # Lower than 0.4 seems to turn off the backlight
    if bl < 0.4:
        bl = 0.4
    if bl > 0.99:
        bl = 1.0
    return bl


def clearScreen():
    global display
    display.set_pen(0)
    display.clear()


def displayScore(p1s, p2s):
    global display
    display.set_pen(255)
    display.text(f"{p1s}", 0, 0, scale=4)
    display.set_pen(28)
    # Change the 70 to fix the positioning
    display.text("First", 70, 0, scale=5)
    # Change the 100 to fix the positioning
    display.text("Last", 100, 40, scale=5)
    display.set_pen(255)
    display.text(f"{p2s}", 280, 0, scale=4)
    display.line(0, 90, 320, 90)


def displayPlayers(p1h, p2h):
    global display
    display.set_pen(224)
    display.rectangle(0, p1h, 20, 50)
    display.rectangle(300, p2h, 20, 50)


def moveBall(x, y, l, u, p1s, p2s):
    if l is False:
        # We're moving right
        if x > 280:
            x = 160
            y = 120
            p1s.addScore()
            l = True if random.random() <= 0.5 else False
            u = True if random.random() <= 0.5 else False
        else:
            x += 1
    else:
        if x < 20:
            x = 160
            y = 120
            p2s.addScore()
            l = True if random.random() <= 0.5 else False
            u = True if random.random() <= 0.5 else False
        else:
            x -= 1

    if u is False:
        # We're going down
        if y > 220:
            u = True
            y -= 1
        else:
            y += 1
    else:
        if y < 92:
            u = False
            y += 1
        else:
            y -= 1
    return x, y, l, u


def displayBall(x, y, color):
    global display
    display.set_pen(color)
    display.rectangle(x, y, 20, 20)


def detectCollision(x, y, p1c, p2c, l):
    if x == 20:
        if p1c.collision(y):
            l = not l
    if x == 280:
        if p2c.collision(y):
            l = not l
    return l


# Do the basic initialization
random.seed()
display.set_backlight(0.5)
display.set_font("bitmap8")
lux_pwr = Pin(27, Pin.OUT)
lux_pwr.value(1)
lux = ADC(26)
p1 = Player(0)
p2 = Player(300)
lorr = True if random.random() <= 0.5 else False
uord = True if random.random() <= 0.5 else False
x = 160
y = 100
c = [224, 236, 252, 28, 3, 102, 66]
index = 0
count = 0
high = 15
while True:
    clearScreen()
    displayScore(p1.getScore(), p2.getScore())
    p1.move()
    p2.move()
    x, y, lorr, uord = moveBall(x, y, lorr, uord, p1, p2)
    displayPlayers(p1.getPosition(), p2.getPosition())
    lorr = detectCollision(x, y, p1, p2, lorr)
    displayBall(x, y, c[index])
    count += 1
    if count == 10:
        index += 1
        count = 0
        if index > len(c) - 1:
            index = 0
        display.set_backlight(getBacklightLevel(lux))
    if p1.getScore() == high or p2.getScore() == high:
        clearScreen()
        display.set_pen(255)
        winner = "Player 1" if p1.getScore() == high else "Player 2"
        display.text(f"{winner} wins!", 5, 100, scale=5)
        display.update()
        time.sleep(3)
        p1.reset()
        p2.reset()
        x = 160
        y = 100
    display.update()
