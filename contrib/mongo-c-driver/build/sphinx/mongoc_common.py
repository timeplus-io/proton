import os
import re
from pathlib import Path
from typing import Any, Iterable, Sequence, Union, List, Tuple, Dict

from docutils import nodes
from docutils.nodes import Node, document

from sphinx.application import Sphinx
from sphinx.application import logger as sphinx_log
try:
    from sphinx.builders.dirhtml import DirectoryHTMLBuilder
except ImportError:
    # Try importing from older Sphinx version path.
    from sphinx.builders.html import DirectoryHTMLBuilder
from sphinx.config import Config
from docutils.parsers.rst import Directive

needs_sphinx = "1.7" # Do not require newer sphinx. EPEL packages build man pages with Sphinx 1.7.6. Refer: CDRIVER-4767
author = "MongoDB, Inc"

# -- Options for HTML output ----------------------------------------------

smart_quotes = False
html_show_sourcelink = False

# Note: http://www.sphinx-doc.org/en/1.5.1/config.html#confval-html_copy_source
# This will degrade the Javascript quicksearch if we ever use it.
html_copy_source = False


def _file_man_page_name(fpath: Path) -> Union[str, None]:
    "Given an rST file input, find the :man_page: frontmatter value, if present"
    lines = fpath.read_text().splitlines()
    for line in lines:
        mat = re.match(r":man_page:\s+(.+)", line)
        if not mat:
            continue
        return mat[1]

def _collect_man (app: Sphinx):
    # Note: 'app' is partially-formed, as this is called from the Sphinx.__init__
    docdir = Path(app.srcdir)
    # Find everything:
    children = docdir.rglob("*")
    # Find only regular files:
    files = filter(Path.is_file, children)
    # Find files that have a .rst extension:
    rst_files = (f for f in files if f.suffix == ".rst")
    # Pair each file with its :man_page: frontmatter, if present:
    with_man_name = ((f, _file_man_page_name(f)) for f in rst_files)
    # Filter out pages that do not have a :man_page: item:s
    pairs: Iterable[tuple[Path, str]] = ((f, m) for f, m in with_man_name if m)
    # Populate the man_pages:
    for filepath, man_name in pairs:
        # Generate the docname.
        relative_path = filepath.relative_to(app.srcdir)
        # The docname is relative to the source directory, and excludes the suffix.
        docname = str(relative_path.parent / filepath.stem)

        assert docname, filepath
        man_pages.append((docname, man_name, "", [author], 3))

# -- Options for manual page output ---------------------------------------

# NOTE: This starts empty, but we populate it in `setup` in _collect_man() (see above)
man_pages: List[Tuple[str, str, str, List[str], int]] = []

# If true, show URL addresses after external links.
#
# man_show_urls = False

# -- Sphinx customization ---------------------------------------


def add_ga_javascript(app: Sphinx, pagename: str, templatename: str, context: Dict[str, Any], doctree: document):
    if not app.env.config.analytics:
        return

    # Add google analytics and NPS survey.
    context["metatags"] = (
        context.get("metatags", "")
        + """
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-56KD6L3MDX"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());

  gtag('config', 'G-56KD6L3MDX');
</script>
<!--  NPS survey -->
<script type="text/javascript">
  !function(e,t,r,n,a){if(!e[a]){for(var i=e[a]=[],s=0;s<r.length;s++){var c=r[s];i[c]=i[c]||function(e){return function(){var t=Array.prototype.slice.call(arguments);i.push([e,t])}}(c)}i.SNIPPET_VERSION="1.0.1";var o=t.createElement("script");o.type="text/javascript",o.async=!0,o.src="https://d2yyd1h5u9mauk.cloudfront.net/integrations/web/v1/library/"+n+"/"+a+".js";var l=t.getElementsByTagName("script")[0];l.parentNode.insertBefore(o,l)}}(window,document,["survey","reset","config","init","set","get","event","identify","track","page","screen","group","alias"],"Dk30CC86ba0nATlK","delighted");

  delighted.survey();
</script>
"""
    )


class VersionList(Directive):
    """Custom directive to generate the version list from an environment variable"""

    option_spec = {}
    has_content = True

    def run(self) -> Sequence[Node]:
        if self.content[0] != "libmongoc" and self.content[0] != "libbson":
            print("versionlist must be libmongoc or libbson")
            return []

        libname = self.content[0]
        env_name = libname.upper() + "_VERSION_LIST"
        if env_name not in os.environ:
            print(env_name + " not set, not generating version list")
            return []

        versions = os.environ[env_name].split(",")

        header = nodes.paragraph("", "")
        p = nodes.paragraph("", "")
        uri = "https://www.mongoc.org/%s/%s/index.html" % (libname, versions[0])
        p += nodes.reference("", "Latest Release (%s)" % versions[0], internal=False, refuri=uri)
        header += p
        p = nodes.paragraph("", "")
        uri = "https://s3.amazonaws.com/mciuploads/mongo-c-driver/docs/%s/latest/index.html" % (libname)
        p += nodes.reference("", "Current Development (master)", internal=False, refuri=uri)
        header += p

        blist = nodes.bullet_list()
        for v in versions:
            item = nodes.list_item()
            p = nodes.paragraph("", "")
            uri = "https://www.mongoc.org/%s/%s/index.html" % (libname, v)
            p += nodes.reference("", v, internal=False, refuri=uri)
            item += p
            blist += item
        return [header, blist]


def generate_html_redirs(app: Sphinx, page: str, templatename: str, context: Dict[str, Any], doctree: Any) -> None:
    builder = app.builder
    if not isinstance(builder, DirectoryHTMLBuilder) or "writing-redirect" in context:
        return
    if page == "index" or page.endswith(".index"):
        return
    path = app.project.doc2path(page, absolute=True)
    out_index_html = Path(builder.get_outfilename(page))
    slug = out_index_html.parent.name
    redirect_file = out_index_html.parent.parent / f"{slug}.html"
    # HACK: handle_page() is not properly reentrant. Save and restore state for
    # this page while we generate our redirects page:
    prev_scripts = builder.script_files[:]
    prev_css = builder.css_files[:]
    builder.handle_page(
        f"redirect-for-{page}",
        {"target": page, "writing-redirect": 1},
        str(Path(__file__).parent.resolve() / "redirect.t.html"),
        str(redirect_file),
    )
    # Restore prior state:
    builder.script_files[:] = prev_scripts
    builder.css_files[:] = prev_css
    sphinx_log.debug("Wrote redirect: %r -> %r", path, page)

def mongoc_common_setup(app: Sphinx):
    _collect_man(app)
    app.connect("html-page-context", generate_html_redirs)
    app.connect("html-page-context", add_ga_javascript)
    # Run sphinx-build -D analytics=1 to enable Google Analytics.
    app.add_config_value("analytics", False, "html")
    app.add_directive("versionlist", VersionList)
