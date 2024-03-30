import gc
import random
import time
from typing import List

import qrcode
from machine import ADC, Pin
from micropython import const
from picographics import DISPLAY_TUFTY_2040, PicoGraphics
from pimoroni import Button

display: PicoGraphics = PicoGraphics(display=DISPLAY_TUFTY_2040)
WIDTH, HEIGHT = display.get_bounds()
button_a: Button = Button(7, invert=False)
button_b: Button = Button(8, invert=False)
button_c: Button = Button(9, invert=False)
button_up: Button = Button(22, invert=False)
button_down: Button = Button(6, invert=False)

# Set this to whatever text you want the QR code to be.
qrtext: str = "https://github.com/DNSGeek/Random-Stuff/blob/master/pong.py"

# Set your names here. It will be autoscaled and centered
firstname: str = "FIRSTNAME"
lastname: str = "LASTNAME"
namesize: int = 5
pheight = const(50)


class Player:
    def __init__(self, xp: int) -> None:
        self.xpos: int = xp
        self.ypos: int = 130
        self.score: int = 0
        self.ybottom: int = self.ypos + pheight

    def reset(self) -> None:
        self.ypos = 130
        self.score = 0
        self.ybottom = self.ypos + pheight

    def moveUp(self) -> None:
        if self.ypos > 92:
            self.ypos -= 1
        self.ybottom = self.ypos + pheight

    def moveDown(self) -> None:
        if self.ypos < 190:
            self.ypos += 1
        self.ybottom = self.ypos + pheight

    def collision(self, bally: int) -> bool:
        ballybot: int = bally + 20
        if self.ypos <= bally <= self.ybottom:
            return True
        if self.ypos <= ballybot <= self.ybottom:
            return True
        return False

    def move(self, lr: bool, bally: int) -> None:
        if (self.xpos == 0 and lr) or (self.xpos == 300 and not lr):
            ballybot: int = bally + 20
            silly: bool = random.random() > 0.85
            if self.ybottom < bally:
                if silly:
                    self.moveUp()
                else:
                    self.moveDown()
            if self.ypos > ballybot:
                if silly:
                    self.moveDown()
                else:
                    self.moveUp()

    def addScore(self) -> None:
        self.score += 1

    def getScore(self) -> int:
        return self.score

    def getPosition(self) -> int:
        return self.ypos


def getBacklightLevel(light) -> float:
    # Keep the display dim to save battery
    reading: float = float(light.read_u16())
    # Values seem to be between 0.0 - 25000.0
    bl: float = (reading / 25000.0) + 0.1
    # Lower than 0.4 seems to turn off the backlight
    bl = max(bl, 0.4)
    bl = min(bl, 1.0)
    return bl


def clearScreen() -> None:
    display.set_pen(0)
    display.clear()


def computeNameSize() -> None:
    global namesize
    maxwidth = const(240)
    namesize = 5
    fnamewidth: int = display.measure_text(firstname, namesize)
    lnamewidth: int = display.measure_text(lastname, namesize)
    namewidth: int = max(fnamewidth, lnamewidth)
    while namewidth > maxwidth:
        if namesize == 1:
            return
        namesize -= 1
        fnamewidth = display.measure_text(firstname, namesize)
        lnamewidth = display.measure_text(lastname, namesize)
        namewidth = max(fnamewidth, lnamewidth)


def displayScore(p1s: int, p2s: int) -> None:
    fnamewidth: int = display.measure_text(firstname, namesize)
    lnamewidth: int = display.measure_text(lastname, namesize)
    fnoffset = ((240 - fnamewidth) // 2) + 40
    lnoffset = ((240 - lnamewidth) // 2) + 40
    display.set_pen(255)
    display.text(f"{p1s}", 0, 0, scale=4)
    display.set_pen(28)
    display.text(firstname, fnoffset, 0, scale=namesize)
    display.text(lastname, lnoffset, 40, scale=namesize)
    display.set_pen(255)
    display.text(f"{p2s}", 280, 0, scale=4)
    display.line(0, 90, 320, 90)


def displayPlayers(p1h: int, p2h: int) -> None:
    display.set_pen(224)
    display.rectangle(0, p1h, 20, 50)
    display.rectangle(300, p2h, 20, 50)


def moveBall(
    bx: int, by: int, lr: bool, ud: bool, p1s: Player, p2s: Player
) -> (int, int, bool, bool):
    if lr is False:
        # We're moving right
        if bx > 280:
            bx = 160
            by = 120
            p1s.addScore()
            lr = random.random() <= 0.5
            ud = random.random() <= 0.5
        else:
            bx += 1
    else:
        if bx < 20:
            bx = 160
            by = 120
            p2s.addScore()
            lr = random.random() <= 0.5
            ud = random.random() <= 0.5
        else:
            bx -= 1

    if ud is False:
        if by > 220:
            ud = True
            by -= 1
        else:
            by += 1
    else:
        if by < 92:
            ud = False
            by += 1
        else:
            by -= 1
    return bx, by, lr, ud


def displayBall(bx: int, by: int, color: int) -> None:
    display.set_pen(color)
    display.rectangle(bx, by, 20, 20)


def detectCollision(
    bx: int, by: int, p1c: Player, p2c: Player, lr: bool
) -> bool:
    if bx == 20:
        if p1c.collision(by):
            lr = not lr
    if bx == 280:
        if p2c.collision(by):
            lr = not lr
    return lr


def showQRCode(lx) -> None:
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
    for qx in range(xs - 1):
        for qy in range(ys - 1):
            borw: bool = code.get_module(qx, qy)
            xp: int = qx * pixel_size
            yp: int = qy * pixel_size
            display.set_pen(255 if borw else 0)
            display.rectangle(
                xp + offset_x, yp + offset_y, pixel_size, pixel_size
            )
    display.set_backlight(1.0)
    display.update()
    time.sleep(10)
    display.set_backlight(getBacklightLevel(lx))


# Do the basic initialization
gc.collect()
gc.threshold(gc.mem_free() // 4 + gc.mem_alloc())
random.seed()
computeNameSize()
display.set_font("bitmap8")
lux_pwr = Pin(27, Pin.OUT)
lux_pwr.value(1)
del(lux_pwr)  # memory is tight, remove anything not needed
lux = ADC(26)
display.set_backlight(getBacklightLevel(lux))
p1: Player = Player(0)
p2: Player = Player(300)
lorr: bool = random.random() <= 0.5
uord: bool = random.random() <= 0.5
x: int = 160
y: int = 100
# [red, orange, yellow, green. blue, indigo, violet]
c: List[int] = [224, 236, 252, 28, 3, 102, 66]
index: int = 0
count: int = 0
high = const(15)
while True:
    clearScreen()
    displayScore(p1.getScore(), p2.getScore())
    p1.move(lorr, y)
    p2.move(lorr, y)
    x, y, lorr, uord = moveBall(x, y, lorr, uord, p1, p2)
    displayPlayers(p1.getPosition(), p2.getPosition())
    lorr = detectCollision(x, y, p1, p2, lorr)
    displayBall(x, y, c[index])
    count = (count + 1) % 10
    if not count:
        index = (index + 1) % 7
        display.set_backlight(getBacklightLevel(lux))
        gc.collect()
        gc.threshold(gc.mem_free() // 4 + gc.mem_alloc())
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
        gc.collect()
        gc.threshold(gc.mem_free() // 4 + gc.mem_alloc())
    display.update()
    if (
        button_a.is_pressed
        or button_b.is_pressed
        or button_c.is_pressed
        or button_up.is_pressed
        or button_down.is_pressed
    ):
        showQRCode(lux)
