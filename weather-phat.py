#!/home/pi/local/python/python3/bin/python3.7 -O
# -*- coding: utf-8 -*-

import argparse
import fnmatch
import os
import pickle
import signal
import time
from daemonize import Daemonize
from font_fredoka_one import FredokaOne
from inky import InkyPHAT
from PIL import Image, ImageDraw, ImageFont

try:
    import requests
except ImportError:
    exit(
        "This script requires the requests module\nInstall with: sudo pip install requests"
    )


# Replace red with your InkyPHAT color
inky_display = InkyPHAT("red")
inky_display.set_border(inky_display.BLACK)

# Set this to be the temperature to change color
WARNING_TEMP = 80.0

# Query Dark Sky (https://darksky.net/) to scrape current weather data
def get_weather():
    # Set your latitude and longitude here.
    coords = (LAT, LONG)
    # Get a dark sky API key at https://darksky.net/dev
    res = requests.get(
        "https://api.darksky.net/forecast/MY_API_KLEY/%s,%s" % (coords[0], coords[1])
    )
    if res.status_code == 200:
        curr = res.json()["currently"]
        try:
            foo = open("/var/tmp/current.pickle", "wb")
            bar = (time.time(), curr)
            pickle.dump(bar, foo, -1)
            foo.close()
            del bar
        except:
            pass
        return curr
    else:
        return {}


def create_mask(
    source, mask=(inky_display.WHITE, inky_display.BLACK, inky_display.RED)
):
    """Create a transparency mask.

    Takes a paletized source image and converts it into a mask
    permitting all the colours supported by Inky pHAT (0, 1, 2)
    or an optional list of allowed colours.

    :param mask: Optional list of Inky pHAT colours to allow.

    """
    mask_image = Image.new("1", source.size)
    w, h = source.size
    for x in range(w):
        for y in range(h):
            p = source.getpixel((x, y))
            if p in mask:
                mask_image.putpixel((x, y), 255)

    return mask_image


def main():
    logfile = open("/tmp/weather.log", "wt")

    def logger(msg):
        logfile.write("%s: %s\n" % (time.asctime(), str(msg)))
        logfile.flush()

    logger("Starting main.")
    icons = {}
    masks = {}
    # Load our icon files and generate masks
    logger("Loading weather icons.")
    respath = "/home/pi/bin"  # Set this to be where you put the script.
    for root, idir, files in os.walk(f"{respath}/resources"):
        for icon in fnmatch.filter(files, "icon-*.png"):
            icon_name = icon.split(".")[0].split("-")[1]
            icon_image = Image.open(f"{respath}/resources/%s" % icon)
            icons[icon_name] = icon_image
            masks[icon_name] = create_mask(icon_image)
    logger("Created icon map: %s" % str(list(icons.keys())))
    logger("Created masks map: %s" % str(list(masks.keys())))

    # Load the FredokaOne font
    logger("Loading font.")
    font = ImageFont.truetype(FredokaOne, 22)

    # This maps the weather summary from Dark Sky
    # to the appropriate weather icons
    logger("Creating icon mapping.")
    icon_map = {
        "snow": ["snow", "sleet", "blizzard"],
        "rain": ["rain", "drizzle"],
        "cloud": [
            "fog",
            "cloudy",
            "partly-cloudy-day",
            "partly-cloudy-night",
            "overcast",
            "mostly cloudy",
            "partly cloudy",
        ],
        "sun": ["clear-day", "clear-night", "sunny", "clear", "partly sunny"],
        "storm": ["stormy", "storm", "thunderstorm"],
        "wind": ["wind", "windy"],
    }

    while True:
        logger("Top of loop.")
        while time.localtime()[5] != 52:
            time.sleep(0.7)
        logger("Starting update.")
        then = time.time()
        # Dictionaries to store our icons and icon masks in
        weather = {}

        # Get the weather data for the given location
        try:
            then, weather = pickle.load(open("/var/tmp/current.pickle", "rb"))
            if (time.time() - then) > 600.0 or time.localtime()[4] % 10 == 0:
                logger("Updating weather.")
                weather = get_weather()
        except:
            weather = get_weather()

        if weather == {}:
            weather = get_weather()

        logger("Got weather.")
        # Placeholder variables
        humidity = 0
        temperature = 0
        weather_icon = None

        if weather:
            summary = str(weather["icon"]).lower()
            logger("summary = %s" % summary)
            temperature = int(round(weather["apparentTemperature"]))
            humidity = int(round(float(weather["humidity"]) * 100.0))

            for icon in icon_map:
                if summary in icon_map[icon]:
                    weather_icon = str(icon)
                    logger("Chose the %s icon." % weather_icon)
                    break
            del icon

        else:
            time.sleep(5)
            continue

        # Create a new canvas to draw on
        logger("Creating image.")
        try:
            img = Image.open(f"{respath}/resources/backdrop.png")
            draw = ImageDraw.Draw(img)

            # Draw lines to frame the weather data
            draw.line((69, 36, 69, 81))  # Vertical line
            draw.line((31, 35, 184, 35))  # Horizontal top line
            draw.line((69, 58, 174, 58))  # Horizontal middle line
            draw.line((169, 58, 169, 58), 2)  # Red seaweed pixel :D

            # Write text with weather values to the canvas
            draw.text(
                (36, 12),
                time.strftime("%m/%d %I:%M", time.localtime(time.time() + 20)),
                inky_display.WHITE,
                font=font,
            )

            # This assumes your weather is in F. Convert to C as appropriate.
            temp = "%dF %.1fC" % (
                temperature,
                # This converts F to C.
                # int(round(((temp * 9.0) / 5.0) + 32.0)) to convert C to F.
                ((weather["apparentTemperature"] - 32.0) * 5.0 / 9.0),
            )
            draw.text(
                (72, 34),
                temp,
                inky_display.WHITE if temperature < WARNING_TEMP else inky_display.RED,
                font=font,
            )

            draw.text(
                (72, 58), "{}% Humidity".format(humidity), inky_display.WHITE, font=font
            )

            # Draw the current weather icon over the backdrop
            if weather_icon is not None:
                logger("weather_icon = %s" % weather_icon)
                img.paste(icons[weather_icon], (28, 36), masks[weather_icon])

            else:
                draw.text((28, 36), "?", inky_display.RED, font=font)

            # Display the weather data on Inky pHAT
            logger("Displaying image.")
            inky_display.set_image(img)
            inky_display.show()
        except Exception as ex:
            logger(f"Uh oh: {ex}")
            time.sleep(5)
            continue
        logger("Deleting local variables.")
        del draw
        del humidity
        del temperature
        del weather_icon
        del weather
        del img
        logger("End of loop.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="This program will display the current time/weatehr on the InkyPHAT display"
    )
    parser.add_argument(
        "-f",
        "--foreground",
        help="Run in the foreground",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-k",
        "--kill",
        help="Kill the currently running process.",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-r",
        "--restart",
        help="Restart the program",
        required=False,
        default=False,
        action="store_true",
    )
    args = parser.parse_args()

    if args.kill or args.restart:
        if os.path.isfile("/tmp/weather.pid"):
            try:
                pf = open("/tmp/weather.pid", "rt")
                pid = pf.readline().strip()
                pf.close()
                os.remove("/tmp/weather.pid")
            except Exception as ex:
                print(f"Error reading PID: {ex}")
                exit(1)
            print(f"Killing PID {pid}")
            try:
                os.kill(int(pid), signal.SIGINT)
            except Exception as ex:
                print(f"Error killing PID {pid}: {ex}")
                exit(2)
            try:
                os.kill(int(pid), signal.SIGKILL)
            except:
                pass
            if args.kill:
                exit(0)
            print("Restarting.")

    if args.foreground:
        main()

    daemon = Daemonize(
        app="weather-phat.py", pid="/tmp/weather.pid", action=main, foreground=False
    )
    daemon.start()
