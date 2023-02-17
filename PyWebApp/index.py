import logging
import os
from functools import wraps
from typing import Dict

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
# Where are the templates?
TEMPLATE_DIR: str = f"{os.getcwd()}/templates"
# Wanna run ing debug mode?
DEBUG: bool = False


# looks for files in the templates directory
def render(filename: str, data: Dict):
    return django.template.loader.get_template(filename).render(data)


# Cookies can be anything. A str, a list, a dict, whatever you want.
def cookie_work(func):
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        # Get the cookie (if it exists)
        cookie = cherrypy.request.cookie[myApp].value

        # Decode the cookie
        myCookie = eatCookie(cookie)

        # Make any cookie changes here, if needed

        # Make the new cookie to return to the client
        encCookie = makeCookie(myCookie)

        cherrypy.response.cookie[myApp] = encCookie
        cherrypy.response.headers[
            "Cache-Control"
        ] = "no-store, no-cache, must-revalidate, post-check=0, pre-check=0"
        cherrypy.response.headers["Pragma"] = "no-cache"
        # Continue on into the function that was originally called.
        return func(self, myCookie, *args, **kwargs)

    return wrapped


def check_all(klass):
    for attr, method in list(klass.__dict__.items()):
        if hasattr(method, "__call__"):
            if attr != "index":
                method = cookie_work(method)
            setattr(klass, attr, cherrypy.expose(method))
    return klass


@check_all
class Root:
    @cherrypy.tools.gzip()
    # Every function besides index will get cookie_work called automagically
    def index(self):
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
            "request.show_tracebacks": False if not DEBUG else True,
            "checker.on": False if not DEBUG else True,
            "tools.caching.on": False,
            "tools.log_headers.on": True,
            "engine.autoreload_on": True,
        }
    )
    # Tell CherryPy to start
    cherrypy.quickstart(Root(), "/", appconf)
