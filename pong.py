import random
import time

import qrcode
from machine import ADC, Pin
from picographics import DISPLAY_TUFTY_2040, PicoGraphics
from pimoroni import Button

# Change your First and Last name in the function
# displayScore and fix the position to make them centered.


display: PicoGraphics = PicoGraphics(display=DISPLAY_TUFTY_2040)
WIDTH, HEIGHT = display.get_bounds()
button_a: Button = Button(7, invert=False)
button_b: Button = Button(8, invert=False)
button_c: Button = Button(9, invert=False)
button_up: Button = Button(22, invert=False)
button_down: Button = Button(6, invert=False)
qrtext: str = "https://github.com/DNSGeek/Random-Stuff/blob/master/pong.py"


class Player:
    def __init__(self, x: int) -> None:
        self.xpos: int = x
        self.ypos: int = 130
        self.height: int = 50
        self.up: bool = True if random.random() <= 0.5 else False
        self.count: int = 0
        self.score: int = 0
        self.ybottom: int = self.ypos + self.height
        self.smooth: int = 15

    def reset(self) -> None:
        self.ypos = 130
        self.up = True if random.random() <= 0.5 else False
        self.count = 0
        self.score = 0
        self.ybottom = self.ypos + self.height

    def moveUp(self) -> None:
        if self.ypos > 92:
            self.ypos -= 1
        else:
            self.up = False
        self.ybottom = self.ypos + self.height

    def moveDown(self) -> None:
        if self.ypos < 190:
            self.ypos += 1
        else:
            self.up = True
        self.ybottom = self.ypos + self.height

    def collision(self, bally: int) -> bool:
        ballybot: int = bally + 20
        if bally >= self.ypos and bally <= self.ybottom:
            return True
        if ballybot >= self.ypos and ballybot <= self.ybottom:
            return True
        return False

    def move(self) -> None:
        if self.count < self.smooth:
            self.count += 1
            if self.up:
                self.moveUp()
            else:
                self.moveDown()
            return
        self.count = 0
        self.up = True if random.random() <= 0.5 else False

    def addScore(self) -> None:
        self.score += 1

    def getScore(self) -> int:
        return self.score

    def getPosition(self) -> int:
        return self.ypos


def getBacklightLevel(l) -> float:
    # Keep the display dim to save battery
    reading: float = float(l.read_u16())
    # Values seem to be between 0.0 - 25000.0
    bl: float = (reading / 25000.0) + 0.1
    # Lower than 0.4 seems to turn off the backlight
    if bl < 0.4:
        bl = 0.4
    if bl > 0.99:
        bl = 1.0
    return bl


def clearScreen() -> None:
    global display
    display.set_pen(0)
    display.clear()


def displayScore(p1s: int, p2s: int) -> None:
    global display
    display.set_pen(255)
    display.text(f"{p1s}", 0, 0, scale=4)
    display.set_pen(28)
    display.text("FIRSTNAME", 70, 0, scale=5)
    display.text("LASTNAME", 100, 40, scale=5)
    display.set_pen(255)
    display.text(f"{p2s}", 280, 0, scale=4)
    display.line(0, 90, 320, 90)


def displayPlayers(p1h: int, p2h: int) -> None:
    global display
    display.set_pen(224)
    display.rectangle(0, p1h, 20, 50)
    display.rectangle(300, p2h, 20, 50)


def moveBall(
    x: int, y: int, l: bool, u: bool, p1s: Player, p2s: Player
) -> (int, int, bool, bool):
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


def displayBall(x: int, y: int, color: int) -> None:
    global display
    display.set_pen(color)
    display.rectangle(x, y, 20, 20)


def detectCollision(x: int, y: int, p1c: Player, p2c: Player, l: bool) -> bool:
    if x == 20:
        if p1c.collision(y):
            l = not l
    if x == 280:
        if p2c.collision(y):
            l = not l
    return l


def showQRCode(l) -> None:
    global qrtext
    global display
    global WIDTH
    global HEIGHT
    clearScreen()
    code = qrcode.QRCode()
    code.set_text(qrtext)
    xs, ys = code.get_size()
    x_size: int = WIDTH // xs
    y_size: int = HEIGHT // ys
    pixel_size: int = min(x_size, y_size)
    offset_x: int = (WIDTH // 2) - ((xs * pixel_size) // 2)
    offset_y: int = (HEIGHT // 2) - ((ys * pixel_size) // 2)
    display.set_pen(255)
    display.rectangle(0, 0, WIDTH, HEIGHT)
    for x in range(xs - 1):
        for y in range(ys - 1):
            borw: bool = code.get_module(x, y)
            xp: int = x * pixel_size
            yp: int = y * pixel_size
            display.set_pen(255 if borw else 0)
            display.rectangle(
                xp + offset_x, yp + offset_y, pixel_size - 1, pixel_size - 1
            )
    display.set_backlight(1.0)
    display.update()
    time.sleep(10)
    display.set_backlight(getBacklightLevel(l))


# Do the basic initialization
random.seed()
display.set_backlight(0.5)
display.set_font("bitmap8")
lux_pwr = Pin(27, Pin.OUT)
lux_pwr.value(1)
lux = ADC(26)
p1: Player = Player(0)
p2: Player = Player(300)
lorr: bool = True if random.random() <= 0.5 else False
uord: bool = True if random.random() <= 0.5 else False
x: int = 160
y: int = 100
# [red, orange, yellow, green. blue, indigo, violet]
c = [224, 236, 252, 28, 3, 102, 66]
index: int = 0
count: int = 0
high: int = 15
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
    if (
        button_a.is_pressed
        or button_b.is_pressed
        or button_c.is_pressed
        or button_up.is_pressed
        or button_down.is_pressed
    ):
        showQRCode(lux)
