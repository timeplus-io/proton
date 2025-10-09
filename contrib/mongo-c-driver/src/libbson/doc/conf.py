# -*- coding: utf-8 -*-
import os
import os.path
import sys

# Ensure we can import "mongoc" extension module.
this_path = os.path.dirname(__file__)
sys.path.append(os.path.normpath(os.path.join(this_path, "../../../build/sphinx")))

from mongoc_common import *

extensions = [
    "mongoc",
]

# General information about the project.
project = "libbson"
copyright = "2017-present, MongoDB, Inc"
author = "MongoDB, Inc"

version_path = os.path.join(os.path.dirname(__file__), "../../..", "VERSION_CURRENT")
version = open(version_path).read().strip()

language = "en"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
master_doc = "index"

# -- Options for HTML output ----------------------------------------------

html_theme = "furo"
html_title = html_shorttitle = "libbson %s" % version
# html_favicon = None

html_sidebars = {}

html_use_index = False

rst_prolog = rf"""

.. _mongodb_docs_cdriver: https://www.mongodb.com/docs/languages/c/c-driver/current/

.. _errors: https://www.mongodb.com/docs/languages/c/c-driver/current/libbson/tutorials/errors/

"""


def add_canonical_link(app, pagename, templatename, context, doctree):
    link = f'<link rel="canonical" href="https://www.mongoc.org/libbson/current/{pagename}"/>'

    context["metatags"] = context.get("metatags", "") + link


def setup(app):
    mongoc_common_setup(app)
    app.connect("html-page-context", add_canonical_link)
