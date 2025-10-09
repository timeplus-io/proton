"""
This is a miniature Sphinx extension that is only used to permit resolution of
external references to the CMake Sphinx documentation. It defines no additional
directives or object types.

This extension defines a "cmake" domain, which may conflict with a proper CMake
domain plugin. If that is the case, this extention can likely be disabled and
replaced by a more full-featured extension.

"""
from typing import Any, List
from sphinx.application import Sphinx
from sphinx.roles import XRefRole
from sphinx.domains import Domain, ObjType

kinds = [
    "command",
    "cpack_gen",
    "envvar",
    "generator",
    "genex",
    "guide",
    "variable",
    "module",
    "policy",
    "prop_cache",
    "prop_dir",
    "prop_gbl",
    "prop_inst",
    "prop_sf",
    "prop_test",
    "prop_tgt",
    "manual",
]


class CMakeRefDomain(Domain):
    name = "cmake"
    label = "CMake (Minimal)"
    object_types = {k: ObjType(k, k) for k in kinds}
    roles = {k: XRefRole() for k in kinds}
    roles["command"] = XRefRole(fix_parens=True)
    directives = {}
    initial_data: Any = {}

    def merge_domaindata(self, docnames: List[str], otherdata: Any) -> None:
        # We have nothing to do, but this is required for parallel execution
        return


def setup(app: Sphinx):
    app.add_domain(CMakeRefDomain)
    return {
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
