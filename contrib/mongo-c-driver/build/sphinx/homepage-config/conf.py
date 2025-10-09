# -*- coding: utf-8 -*-
from docutils import nodes
import os
import sys

# Import common docs config.
this_path = os.path.dirname(__file__)
sys.path.append(os.path.normpath(os.path.join(this_path, '../')))

from mongoc_common import *

version = release = os.environ.get('VERSION_RELEASED')
if not release:
    raise RuntimeError('Set the "VERSION_RELEASED" environment variable')

# -- General configuration ------------------------------------------------
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'

# General information about the project.
project = u'mongoc.org'
copyright = u'2017, MongoDB, Inc'
author = u'MongoDB, Inc'
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False


# Support :download-link:`bson` or :download-link:`mongoc`.
def download_link(typ, rawtext, text, lineno, inliner, options={}, content=[]):
    if text == "mongoc":
        lib = "mongo-c-driver"
    else:
        raise ValueError(
            "download link must be mongoc, not \"%s\"" % text)

    title = "%s-%s" % (lib, version)
    url = ("https://github.com/mongodb/mongo-c-driver/releases/tag/%(version)s") % {
              "version": version
          }

    pnode = nodes.reference(title, title, internal=False, refuri=url)
    return [pnode], []

def setup(app):
    mongoc_common_setup(app)

    app.add_role('download-link', download_link)

# -- Options for HTML output ----------------------------------------------

html_theme = 'furo'
html_title = html_shorttitle = 'MongoDB C Driver %s' % version
# html_favicon = None
html_use_smartypants = False
html_show_sourcelink = False
html_use_index = False
rst_prolog = rf"""

.. _mongodb_docs_cdriver: https://www.mongodb.com/docs/languages/c/c-driver/current/

"""

html_sidebars = {
    '**': []
}

# Note: http://www.sphinx-doc.org/en/1.5.1/config.html#confval-html_copy_source
# This will degrade the Javascript quicksearch if we ever use it.
html_copy_source = False
