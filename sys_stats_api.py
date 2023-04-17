#!/usr/bin/python3 -uO

from json import dumps
from sys import platform

from flask import Flask

if platform == "darwin":
    from mac_sys_stats import get_top, parse_top
else:
    from linux_sys_stats import get_top, parse_top

app = Flask(__name__)


@app.route("/", methods=["GET"])
def send_stats():
    return dumps(parse_top(get_top()))
