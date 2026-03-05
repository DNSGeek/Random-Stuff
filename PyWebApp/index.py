import logging
import os
import time
from functools import wraps
from typing import Any, Dict, Optional

# Import the CherryPy modules.
import cherrypy

# Import the Django modules.
import django
from django.conf import settings

from cookieMonster import eatCookie, makeCookie

# What is the name of this app?
myApp: str = "myapp"
# What port will we listen on?
PORT: int = 8080
# Set this to the number of seconds before the cookie data
# times out and needs to be renewed. Could be used for an
# auto-logout, for example.
TIMEOUT: int = 900
# Where are the templates?
TEMPLATE_DIR: str = f"{os.getcwd()}/templates"
# Wanna run in debug mode?
DEBUG: bool = False


# looks for files in the templates directory
def render(filename: str, data: Dict) -> str:
    return django.template.loader.get_template(filename).render(data)


def _get_cookie() -> Optional[Any]:
    """Read and decode the app cookie from the current request.
    Returns None if the cookie is absent or invalid."""
    morsel = cherrypy.request.cookie.get(myApp)
    if morsel is None:
        return None
    return eatCookie(morsel.value)


def _refresh_cookie(myCookie: Any) -> None:
    """Update the timeout timestamp and write the cookie to the response."""
    if isinstance(myCookie, dict):
        now: int = int(
            time.time()
        )  # FIX: was time.localtime() which returns struct_time, not int
        cookie_time: int = myCookie.get("__cookieTime__", now)
        if now - cookie_time > TIMEOUT:
            myCookie["__cookieTime__"] = 0
        else:
            myCookie["__cookieTime__"] = now

    encCookie = makeCookie(myCookie)
    cherrypy.response.cookie[myApp] = encCookie
    cherrypy.response.headers["Cache-Control"] = (
        "no-store, no-cache, must-revalidate, post-check=0, pre-check=0"
    )
    cherrypy.response.headers["Pragma"] = "no-cache"


# Cookies can be anything. A str, a list, a dict, whatever you want.
def cookie_work(func):
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        # FIX: was a bare dict lookup that would KeyError if cookie was absent.
        # Now returns None gracefully when the cookie is missing or invalid.
        myCookie = _get_cookie()
        if myCookie is None:
            raise cherrypy.HTTPError(401, "Missing or invalid session cookie.")

        _refresh_cookie(myCookie)

        # Continue on into the function that was originally called.
        return func(self, myCookie, *args, **kwargs)

    return wrapped


def check_all(klass):
    for attr, method in list(klass.__dict__.items()):
        if callable(
            method
        ):  # FIX: hasattr(method, "__call__") is the old way; callable() is idiomatic
            if attr != "index":
                method = cookie_work(method)
            setattr(klass, attr, cherrypy.expose(method))
    return klass


@check_all
class Root:
    @cherrypy.tools.gzip()
    # Every function besides index will get cookie_work called automagically
    def index(self):
        # Initialize the cookie
        encCookie = makeCookie({"my": "data", "__cookieTime__": int(time.time())})
        cherrypy.response.cookie[myApp] = encCookie
        return render(
            "index.html",
            {"message": "Moo", "somelist": ["r", "f", 1, "a", "cruel"]},
        )

    # All other functions need to be "def page(self, cookie, args):"


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.INFO if not DEBUG else logging.DEBUG,
    )

    # Do basic Django settings configuration
    settings.configure(
        BASE_DIR=os.getcwd(),
    )
    settings.TEMPLATES = [
        {
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "DIRS": [
                TEMPLATE_DIR,
            ],
            "APP_DIRS": True,
            "OPTIONS": {
                "context_processors": [
                    "django.template.context_processors.debug",
                    "django.template.context_processors.request",
                    "django.contrib.auth.context_processors.auth",
                    "django.contrib.messages.context_processors.messages",
                ],
            },
        },
    ]
    # Tell Django to start
    django.setup()

    # Set up the CherryPy App
    appconf = {
        "/": {"tools.staticdir.root": os.getcwd()},
        "/static": {
            "tools.staticdir.on": True,
            "tools.staticdir.dir": "static",
        },
    }

    # Do basic CherryPy setup
    cherrypy.config.update(
        {
            "server.socket_host": "0.0.0.0",
            "server.socket_port": PORT,
            "server.ssl_certificate": os.getcwd() + "/ssl/server.crt",
            "server.ssl_private_key": os.getcwd() + "/ssl/server.key",
            "request.show_tracebacks": DEBUG,
            "checker.on": DEBUG,
            "tools.caching.on": False,
            "tools.log_headers.on": True,
            "engine.autoreload_on": True,
        }
    )
    # Tell CherryPy to start
    cherrypy.quickstart(Root(), "/", appconf)
