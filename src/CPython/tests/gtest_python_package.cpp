#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/AsyncPythonPackageManager.h>
#include <CPython/GILGuard.h>
#include <CPython/PythonPackage.h>
#include <CPython/Utils.h>
#include <CPython/tests/CPythonTest.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/scope_guard.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>

using namespace DB::cpython;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{
void expectInvalidPackageSpecification(const std::string & package_spec)
{
    try
    {
        PythonPackage::validatePackageSpecification(package_spec);
        FAIL() << "Expected package specification to be rejected: " << package_spec;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::BAD_ARGUMENTS);
    }
}
}

TEST(PythonPackageTest, AcceptsPackageSpecificationsWithExtras)
{
    EXPECT_NO_THROW(PythonPackage::validatePackageSpecification("confluent-kafka[protobuf,avro]"));
    EXPECT_NO_THROW(PythonPackage::validatePackageSpecification("confluent-kafka[protobuf, avro]"));
    EXPECT_NO_THROW(PythonPackage::validatePackageSpecification("confluent-kafka[protobuf]>=2.1.1"));
    EXPECT_NO_THROW(PythonPackage::validatePackageSpecification("requests[socks]>=2.25.0,<3.0.0"));
}

TEST(PythonPackageTest, RejectsMalformedExtrasSpecifications)
{
    expectInvalidPackageSpecification("confluent-kafka[]");
    expectInvalidPackageSpecification("confluent-kafka[protobuf,]");
    expectInvalidPackageSpecification("confluent-kafka[,protobuf]");
    expectInvalidPackageSpecification("confluent-kafka[protobuf,,avro]");
    expectInvalidPackageSpecification("confluent-kafka[protobuf");
}

TEST(PythonPackageTest, ParsesExtrasAndVersionSpecifiers)
{
    PythonPackage package("confluent-kafka[protobuf, avro]>=2.1.1");

    EXPECT_EQ(package.name, "confluent-kafka[protobuf, avro]");
    EXPECT_EQ(package.lookup_name, "confluent-kafka");
    EXPECT_EQ(package.version_spec, ">=2.1.1");
    EXPECT_EQ(package.package_with_version, "confluent-kafka[protobuf, avro]>=2.1.1");
}

TEST(PythonPackageTest, RemoveCompletedTaskIgnoresUnknownTasks)
{
    AsyncPythonPackageManager manager(nullptr);
    /// Unknown ids are reported as not removed and must not crash or affect tracking.
    EXPECT_FALSE(manager.removeCompletedTask("no-such-task"));
    EXPECT_TRUE(manager.getAllTaskResults().empty());
    manager.shutdown();
}

TEST(PythonPackageTest, HasEffectiveRequirementLinesMatchesParserSkipRules)
{
    EXPECT_FALSE(PythonPackage::hasEffectiveRequirementLines(""));
    EXPECT_FALSE(PythonPackage::hasEffectiveRequirementLines("\n\n"));
    EXPECT_FALSE(PythonPackage::hasEffectiveRequirementLines("# comment only\n  \t\n#another\n"));
    EXPECT_TRUE(PythonPackage::hasEffectiveRequirementLines("requests==2.31.0\n"));
    EXPECT_TRUE(PythonPackage::hasEffectiveRequirementLines("# header\n  six==1.17.0  \n"));
    /// Mirror of the parser's contract: content without effective lines makes parse throw, so
    /// callers must consult this helper first when "nothing to install" is a valid outcome.
    EXPECT_THROW(PythonPackage::parseRequirementsText("# comment only\n"), DB::Exception);
}

TEST(PythonPackageTest, ParsesRequirementsTextWithExtras)
{
    const auto requirements = PythonPackage::parseRequirementsText(R"(
confluent-kafka[protobuf,avro]
confluent-kafka[protobuf, avro]>=2.1.1
)");

    ASSERT_EQ(requirements.size(), 2);
    EXPECT_EQ(requirements[0], "confluent-kafka[protobuf,avro]");
    EXPECT_EQ(requirements[1], "confluent-kafka[protobuf, avro]>=2.1.1");
}

TEST_F(CPythonTest, RefreshImportStateProcessesPthFiles)
{
    namespace fs = std::filesystem;

    const auto unique_suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto test_root = fs::absolute(fs::path("tmp") / ("gtest_python_package_refresh_" + unique_suffix));
    const auto user_base = test_root / "userbase";
    const auto site_packages = user_base / "lib/python3.10/site-packages";
    const auto extra_modules = test_root / "extra_modules";
    const auto module_name = "python_package_refresh_" + unique_suffix;

    fs::create_directories(site_packages);
    fs::create_directories(extra_modules);

    {
        std::ofstream module_file(extra_modules / (module_name + ".py"), std::ios::out | std::ios::trunc);
        module_file << "VALUE = 11719\n";
    }

    {
        std::ofstream pth_file(site_packages / (module_name + ".pth"), std::ios::out | std::ios::trunc);
        pth_file << extra_modules.string() << '\n';
    }

    const char * previous_user_base = getenv("PYTHONUSERBASE");
    const std::string previous_user_base_value = previous_user_base ? previous_user_base : "";

    ASSERT_EQ(setenv("PYTHONUSERBASE", user_base.c_str(), 1), 0);
    SCOPE_EXIT({
        if (previous_user_base)
            setenv("PYTHONUSERBASE", previous_user_base_value.c_str(), 1);
        else
            unsetenv("PYTHONUSERBASE");

        std::error_code error_code;
        fs::remove_all(test_root, error_code);
    });

    GILGuard gil_guard(true);

    auto before_refresh = PyObjectPtr{PyImport_ImportModule(module_name.c_str())};
    EXPECT_FALSE(before_refresh);
    if (PyErr_Occurred())
        PyErr_Clear();

    ASSERT_NO_THROW(PythonPackage::refreshImportState(getLogger("PythonPackageTest")));

    auto module = PyObjectPtr{PyImport_ImportModule(module_name.c_str())};
    ASSERT_TRUE(module) << getExceptionMessage();

    auto value = PyObjectPtr{PyObject_GetAttrString(module.get(), "VALUE")};
    ASSERT_TRUE(value);
    EXPECT_EQ(PyLong_AsLong(value.get()), 11719);

    unloadModule(module_name);
}

TEST_F(CPythonTest, RefreshImportStateProcessesDefaultUserSiteWhenPyUserBaseUnset)
{
    namespace fs = std::filesystem;

    const auto unique_suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto test_root = fs::absolute(fs::path("tmp") / ("gtest_python_package_default_user_site_" + unique_suffix));
    const auto user_base = test_root / "userbase";
    const auto user_site = user_base / "lib/python3.10/site-packages";
    const auto extra_modules = test_root / "extra_modules";
    const auto module_name = "python_package_default_user_site_" + unique_suffix;

    fs::create_directories(user_site);
    fs::create_directories(extra_modules);

    {
        std::ofstream module_file(extra_modules / (module_name + ".py"), std::ios::out | std::ios::trunc);
        module_file << "VALUE = 11726\n";
    }

    {
        std::ofstream pth_file(user_site / (module_name + ".pth"), std::ios::out | std::ios::trunc);
        pth_file << extra_modules.string() << '\n';
    }

    const char * previous_user_base_env = getenv("PYTHONUSERBASE");
    const std::string previous_user_base_value = previous_user_base_env ? previous_user_base_env : "";

    ASSERT_EQ(unsetenv("PYTHONUSERBASE"), 0);

    GILGuard gil_guard(true);

    auto site_module = PyObjectPtr{PyImport_ImportModule("site")};
    ASSERT_TRUE(site_module) << getExceptionMessage();

    auto previous_user_base = PyObjectPtr{PyObject_GetAttrString(site_module.get(), "USER_BASE")};
    ASSERT_TRUE(previous_user_base) << getExceptionMessage();
    auto previous_user_site = PyObjectPtr{PyObject_GetAttrString(site_module.get(), "USER_SITE")};
    ASSERT_TRUE(previous_user_site) << getExceptionMessage();

    auto user_base_obj = PyObjectPtr{PyUnicode_FromString(user_base.c_str())};
    auto user_site_obj = PyObjectPtr{PyUnicode_FromString(user_site.c_str())};
    ASSERT_TRUE(user_base_obj);
    ASSERT_TRUE(user_site_obj);
    ASSERT_EQ(PyObject_SetAttrString(site_module.get(), "USER_BASE", user_base_obj.get()), 0) << getExceptionMessage();
    ASSERT_EQ(PyObject_SetAttrString(site_module.get(), "USER_SITE", user_site_obj.get()), 0) << getExceptionMessage();

    auto sys_module = PyObjectPtr{PyImport_ImportModule("sys")};
    ASSERT_TRUE(sys_module);
    auto sys_path = PyObjectPtr{PyObject_GetAttrString(sys_module.get(), "path")};
    ASSERT_TRUE(sys_path);
    auto remove_func = PyObjectPtr{PyObject_GetAttrString(sys_path.get(), "remove")};

    SCOPE_EXIT({
        if (previous_user_base_env)
            setenv("PYTHONUSERBASE", previous_user_base_value.c_str(), 1);
        else
            unsetenv("PYTHONUSERBASE");

        PyObject_SetAttrString(site_module.get(), "USER_BASE", previous_user_base.get());
        PyObject_SetAttrString(site_module.get(), "USER_SITE", previous_user_site.get());
        if (PyErr_Occurred())
            PyErr_Clear();

        if (remove_func)
        {
            auto unused = PyObjectPtr{PyObject_CallFunctionObjArgs(remove_func.get(), user_site_obj.get(), nullptr)};
            (void)unused;
        }
        if (PyErr_Occurred())
            PyErr_Clear();

        unloadModule(module_name);

        std::error_code error_code;
        fs::remove_all(test_root, error_code);
    });

    auto before_refresh = PyObjectPtr{PyImport_ImportModule(module_name.c_str())};
    EXPECT_FALSE(before_refresh);
    if (PyErr_Occurred())
        PyErr_Clear();

    ASSERT_NO_THROW(PythonPackage::refreshImportState(getLogger("PythonPackageTest")));

    auto module = PyObjectPtr{PyImport_ImportModule(module_name.c_str())};
    ASSERT_TRUE(module) << getExceptionMessage();

    auto value = PyObjectPtr{PyObject_GetAttrString(module.get(), "VALUE")};
    ASSERT_TRUE(value);
    EXPECT_EQ(PyLong_AsLong(value.get()), 11726);
}

TEST_F(CPythonTest, RefreshImportStateExtendsNamespacePackagePaths)
{
    namespace fs = std::filesystem;

    /// Skip if pkg_resources is not available (needed for declare_namespace).
    /// Must hold the GIL: an earlier test may have released it via PyEval_SaveThread.
    {
        GILGuard gil_guard(true);
        auto pkg_resources_module = PyObjectPtr{PyImport_ImportModule("pkg_resources")};
        if (!pkg_resources_module)
        {
            PyErr_Clear();
            GTEST_SKIP() << "pkg_resources not available, skipping namespace package test";
        }
    }

    const auto unique_suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto test_root = fs::absolute(fs::path("tmp") / ("gtest_python_package_ns_" + unique_suffix));
    const auto user_base = test_root / "userbase";
    const auto site_packages = user_base / "lib/python3.10/site-packages";

    const auto dir1 = test_root / "dir1";
    const auto dir2 = test_root / "dir2";
    const auto ns_name = "ns_pkg_" + unique_suffix;

    fs::create_directories(site_packages);
    fs::create_directories(dir1 / ns_name);
    fs::create_directories(dir2 / ns_name);

    {
        std::ofstream f(dir1 / ns_name / "__init__.py");
        f << "import pkg_resources; pkg_resources.declare_namespace(__name__)\n";
    }
    {
        std::ofstream f(dir1 / ns_name / "child_a.py");
        f << "VALUE = 100\n";
    }
    {
        std::ofstream f(dir2 / ns_name / "child_b.py");
        f << "VALUE = 200\n";
    }
    {
        std::ofstream f(site_packages / (ns_name + ".pth"));
        f << dir2.string() << '\n';
    }

    const char * previous_user_base = getenv("PYTHONUSERBASE");
    const std::string previous_user_base_value = previous_user_base ? previous_user_base : "";

    ASSERT_EQ(setenv("PYTHONUSERBASE", user_base.c_str(), 1), 0);
    SCOPE_EXIT({
        if (previous_user_base)
            setenv("PYTHONUSERBASE", previous_user_base_value.c_str(), 1);
        else
            unsetenv("PYTHONUSERBASE");

        std::error_code ec;
        fs::remove_all(test_root, ec);
    });

    GILGuard gil_guard(true);

    auto sys_module = PyObjectPtr{PyImport_ImportModule("sys")};
    ASSERT_TRUE(sys_module);
    auto sys_path = PyObjectPtr{PyObject_GetAttrString(sys_module.get(), "path")};
    ASSERT_TRUE(sys_path);
    auto dir1_str = PyObjectPtr{PyUnicode_FromString(dir1.c_str())};
    PyList_Insert(sys_path.get(), 0, dir1_str.get());

    const auto child_a_fqn = ns_name + ".child_a";
    auto mod_a = PyObjectPtr{PyImport_ImportModule(child_a_fqn.c_str())};
    ASSERT_TRUE(mod_a) << getExceptionMessage();
    auto val_a = PyObjectPtr{PyObject_GetAttrString(mod_a.get(), "VALUE")};
    ASSERT_TRUE(val_a);
    EXPECT_EQ(PyLong_AsLong(val_a.get()), 100);

    const auto child_b_fqn = ns_name + ".child_b";
    auto mod_b_before = PyObjectPtr{PyImport_ImportModule(child_b_fqn.c_str())};
    EXPECT_FALSE(mod_b_before) << "child_b should not be importable before refresh";
    if (PyErr_Occurred())
        PyErr_Clear();

    ASSERT_NO_THROW(PythonPackage::refreshImportState(getLogger("PythonPackageTest")));

    auto mod_b = PyObjectPtr{PyImport_ImportModule(child_b_fqn.c_str())};
    ASSERT_TRUE(mod_b) << "child_b should be importable after refresh: " << getExceptionMessage();
    auto val_b = PyObjectPtr{PyObject_GetAttrString(mod_b.get(), "VALUE")};
    ASSERT_TRUE(val_b);
    EXPECT_EQ(PyLong_AsLong(val_b.get()), 200);

    PyObject * modules_dict = PyImport_GetModuleDict();
    auto ns_key = PyObjectPtr{PyUnicode_FromString(ns_name.c_str())};
    auto a_key = PyObjectPtr{PyUnicode_FromString(child_a_fqn.c_str())};
    auto b_key = PyObjectPtr{PyUnicode_FromString(child_b_fqn.c_str())};
    PyDict_DelItem(modules_dict, ns_key.get());
    PyDict_DelItem(modules_dict, a_key.get());
    PyDict_DelItem(modules_dict, b_key.get());
    if (PyErr_Occurred())
        PyErr_Clear();

    auto pkg_resources_module = PyObjectPtr{PyImport_ImportModule("pkg_resources")};
    ASSERT_TRUE(pkg_resources_module);
    auto ns_packages_dict = PyObjectPtr{PyObject_GetAttrString(pkg_resources_module.get(), "_namespace_packages")};
    if (ns_packages_dict && PyDict_Check(ns_packages_dict.get()))
        PyDict_DelItem(ns_packages_dict.get(), ns_key.get());
    if (PyErr_Occurred())
        PyErr_Clear();

    auto remove_func = PyObjectPtr{PyObject_GetAttrString(sys_path.get(), "remove")};
    if (remove_func)
    {
        auto unused = PyObjectPtr{PyObject_CallFunctionObjArgs(remove_func.get(), dir1_str.get(), nullptr)};
        (void)unused;
    }
    if (PyErr_Occurred())
        PyErr_Clear();
}

#endif
