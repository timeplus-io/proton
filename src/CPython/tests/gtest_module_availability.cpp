#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <CPython/tests/CPythonTest.h>

#include <patchlevel.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace DB::cpython;

/// ---------------------------------------------------------------------------
/// Bootstrap module availability tests
///
/// CPython requires a set of built-in C extension modules for basic
/// interpreter operation (defined in Modules/Setup.bootstrap.in).
/// When upgrading CPython versions, new modules are often added to this
/// list.  If cpython-cmake doesn't compile and register them, the
/// interpreter appears to work but stdlib imports fail at runtime in
/// surprising ways (e.g. "No module named '_tokenize'" when importlib
/// calls invalidate_caches).
///
/// These tests import each module from the embedded interpreter and
/// fail loudly when one is missing — catching the problem at unit-test
/// time rather than in production.
/// ---------------------------------------------------------------------------

/// Helper: try to import a module, return error message or empty string.
static std::string tryImportModule(const char * name)
{
    PyObjectPtr mod{PyImport_ImportModule(name)};
    if (mod)
        return {};
    std::string err;
    if (PyErr_Occurred())
    {
        PyObject * ptype = nullptr;
        PyObject * pvalue = nullptr;
        PyObject * ptb = nullptr;
        PyErr_Fetch(&ptype, &pvalue, &ptb);
        if (pvalue)
        {
            PyObjectPtr str{PyObject_Str(pvalue)};
            if (str)
                err = PyUnicode_AsUTF8(str.get());
        }
        Py_XDECREF(ptype);
        Py_XDECREF(pvalue);
        Py_XDECREF(ptb);
    }
    return err.empty() ? "unknown import error" : err;
}

/// ---------------------------------------------------------------------------
/// Test: all CPython bootstrap modules are importable.
/// These are required for the interpreter to function at all.
/// Source of truth: Modules/Setup.bootstrap.in
/// ---------------------------------------------------------------------------
TEST_F(CPythonTest, BootstrapModulesAvailable)
{
    GILGuard gil(true);

    // Modules/Setup.bootstrap.in — required for a functioning interpreter
    const std::vector<std::string> bootstrap_modules = {
        "atexit",
        "faulthandler",
        "posix",
        "_signal",
        "_tracemalloc",
        "_datetime",
        "_codecs",
        "_collections",
        "errno",
        "_io",
        "itertools",
        "_sre",
        "_thread",
        "time",
        "_weakref",
        "_abc",
        "_functools",
        "_locale",
        "_opcode",
        "_operator",
        "_stat",
        "_symtable",
        "pwd",
#if PY_VERSION_HEX >= 0x030E0000 // 3.14+
        "_suggestions",
        "_sysconfig",
        "_types",
        "_typing",
#endif
    };

    for (const auto & name : bootstrap_modules)
    {
        auto err = tryImportModule(name.c_str());
        EXPECT_TRUE(err.empty()) << "Bootstrap module '" << name << "' failed to import: " << err;
    }
}

/// ---------------------------------------------------------------------------
/// Test: core modules registered in config.c are importable.
/// These live in Python/ (not Modules/) and are registered manually.
/// ---------------------------------------------------------------------------
TEST_F(CPythonTest, CoreModulesAvailable)
{
    GILGuard gil(true);

    // Registered in config_3.c.in — core interpreter modules
    const std::vector<std::string> core_modules = {
        "marshal",
        "_imp",
        "_ast",
        "gc",
        "_warnings",
        "_string",
        "_signal",
#if PY_VERSION_HEX >= 0x030E0000 // 3.14+
        "_tokenize",
        "_contextvars",
#endif
    };

    for (const auto & name : core_modules)
    {
        auto err = tryImportModule(name.c_str());
        EXPECT_TRUE(err.empty()) << "Core module '" << name << "' failed to import: " << err;
    }
}

/// ---------------------------------------------------------------------------
/// Test: hash/crypto extension modules (HACL* backed in 3.14+).
/// ---------------------------------------------------------------------------
TEST_F(CPythonTest, HashModulesAvailable)
{
    GILGuard gil(true);

    const std::vector<std::string> hash_modules = {
        "_md5",
        "_sha1",
        "_blake2",
        "_sha3",
#if PY_VERSION_HEX >= 0x030E0000
        "_sha2",   // merged from _sha256 + _sha512 in 3.14
#endif
        "hashlib",
    };

    for (const auto & name : hash_modules)
    {
        auto err = tryImportModule(name.c_str());
        EXPECT_TRUE(err.empty()) << "Hash module '" << name << "' failed to import: " << err;
    }
}

/// ---------------------------------------------------------------------------
/// Test: stdlib modules commonly used by Python UDFs and pip.
/// If these fail, Python UDFs or package installation will break.
/// ---------------------------------------------------------------------------
TEST_F(CPythonTest, StdlibModulesUsedByUDFs)
{
    GILGuard gil(true);

    const std::vector<std::string> stdlib_modules = {
        "json",
        "re",
        "math",
        "os",
        "sys",
        "io",
        "collections",
        "functools",
        "typing",
        "importlib",
        "tokenize",
        "threading",
        "zipfile",
        "struct",
        "pickle",
        "datetime",
        "traceback",
    };

    for (const auto & name : stdlib_modules)
    {
        auto err = tryImportModule(name.c_str());
        EXPECT_TRUE(err.empty()) << "Stdlib module '" << name << "' failed to import: " << err;
    }
}

/// ---------------------------------------------------------------------------
/// Test: os module has POSIX identity functions.
/// These are guarded by HAVE_GETUID etc. in pyconfig.h.  Missing defines
/// silently omit the functions, which then break pkg_resources and other
/// stdlib code that assumes a POSIX environment.
/// ---------------------------------------------------------------------------
TEST_F(CPythonTest, PosixIdentityFunctionsAvailable)
{
    GILGuard gil(true);

    ASSERT_EQ(PyRun_SimpleString(
        "import os\n"
        "assert hasattr(os, 'getuid'),  'os.getuid missing — check HAVE_GETUID in pyconfig.h'\n"
        "assert hasattr(os, 'getgid'),  'os.getgid missing — check HAVE_GETGID in pyconfig.h'\n"
        "assert hasattr(os, 'geteuid'), 'os.geteuid missing — check HAVE_GETEUID in pyconfig.h'\n"
        "assert hasattr(os, 'getegid'), 'os.getegid missing — check HAVE_GETEGID in pyconfig.h'\n"
        "assert hasattr(os, 'getpid'),  'os.getpid missing — check HAVE_GETPID in pyconfig.h'\n"
        "assert hasattr(os, 'getppid'), 'os.getppid missing — check HAVE_GETPPID in pyconfig.h'\n"
    ), 0) << "POSIX identity functions missing from os module: " << getExceptionMessage();
}

/// ---------------------------------------------------------------------------
/// Test: importlib.invalidate_caches() works without error.
/// This is the exact call chain that failed when _tokenize and
/// _contextvars were missing — it exercises a deep import graph.
/// ---------------------------------------------------------------------------
TEST_F(CPythonTest, ImportlibInvalidateCachesWorks)
{
    GILGuard gil(true);

    ASSERT_EQ(PyRun_SimpleString(
        "import importlib\n"
        "importlib.invalidate_caches()\n"
    ), 0) << "importlib.invalidate_caches() failed: " << getExceptionMessage();
}

/// ---------------------------------------------------------------------------
/// Test: embedded interpreter version matches compiled version.
/// Catches submodule pointer mismatch at test time.
/// ---------------------------------------------------------------------------
TEST_F(CPythonTest, InterpreterVersionMatchesCompiled)
{
    GILGuard gil(true);

    PyObjectPtr sys_mod{PyImport_ImportModule("sys")};
    ASSERT_TRUE(sys_mod);

    PyObjectPtr version_info{PyObject_GetAttrString(sys_mod.get(), "version_info")};
    ASSERT_TRUE(version_info);

    PyObjectPtr major{PyObject_GetAttrString(version_info.get(), "major")};
    PyObjectPtr minor{PyObject_GetAttrString(version_info.get(), "minor")};
    ASSERT_TRUE(major);
    ASSERT_TRUE(minor);

    EXPECT_EQ(PyLong_AsLong(major.get()), PY_MAJOR_VERSION)
        << "Runtime major version doesn't match compiled PY_MAJOR_VERSION";
    EXPECT_EQ(PyLong_AsLong(minor.get()), PY_MINOR_VERSION)
        << "Runtime minor version doesn't match compiled PY_MINOR_VERSION";
}

#endif // USE_PYTHON_UDF
