#pragma once

#include <CPython/PythonInterpreterInfo.h>

#include <Common/Logger.h>

namespace DB::cpython
{
class PythonPackage
{
public:
    PythonPackage(const std::string & package_with_version_)
        : package_with_version(package_with_version_), logger(getLogger("PythonPackage"))
    {
        validatePackageSpecification(package_with_version_);
        parsePackageWithVersion();
    }

    static std::vector<PythonPackage> list();

    /// Get the Python interpreter info used by UDF runtime
    static PythonInterpreterInfo getPythonInterpreterInfo();

    static void ensurePythonInterpreterReady();

    /// Validate package name and specification format (static validation)
    static void validatePackageSpecification(const std::string & package_spec);

    /// Pre-install validation: check if package exists and can be installed (using dry-run)
    void validateInstall(LoggerPtr logger) const;

    /// Pre-uninstall validation: check if package is currently installed (fast check only)
    void validateUninstall(LoggerPtr logger) const;

    void install(LoggerPtr logger) const;
    void uninstall(LoggerPtr logger) const;

    /// Get the actual installed version of a package (returns empty string if not installed)
    std::string getActualInstalledVersion(LoggerPtr logger) const;

    std::string package_with_version;
    std::string name;
    std::string version_spec;
    LoggerPtr logger;

private:
    void parsePackageWithVersion();
};

using PythonPackages = std::vector<PythonPackage>;
}
