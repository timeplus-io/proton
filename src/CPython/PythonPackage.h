#pragma once

#include <CPython/PythonInterpreterInfo.h>

#include <Common/Logger.h>

#include <string>
#include <utility>
#include <vector>

namespace DB::cpython
{
struct PipInstallOptions
{
    std::string index_url;
    std::vector<std::string> extra_index_urls;
};

class PythonPackage
{
public:
    explicit PythonPackage(const std::string & package_with_version_, PipInstallOptions install_options_ = {});

    static PythonPackage fromRequirementsText(const std::string & requirements_text_, PipInstallOptions install_options_ = {});

    static std::vector<PythonPackage> list();

    /// Get the Python interpreter info used by UDF runtime
    static PythonInterpreterInfo getPythonInterpreterInfo();

    static void ensurePythonInterpreterReady();

    /// Validate package name and specification format (static validation)
    static void validatePackageSpecification(const std::string & package_spec);

    /// Refresh site-packages and import caches after package installation.
    static void refreshImportState(LoggerPtr logger = {});

    /// Validate pip private index URL format.
    static void validatePipIndexUrl(const std::string & index_url, const char * option_name);

    /// True when the text contains at least one non-blank, non-comment line. Shares the parser's
    /// line-skipping rules so "nothing to install" checks cannot drift from parseRequirementsText
    /// (which throws on such content instead of returning an empty list).
    static bool hasEffectiveRequirementLines(const std::string & requirements_text);

    /// Validate requirements text content.
    static std::vector<std::string> parseRequirementsText(const std::string & requirements_text);

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
    std::string lookup_name;
    std::string version_spec;
    std::string requirements_text;
    bool install_from_requirements = false;
    PipInstallOptions install_options;
    LoggerPtr logger;

private:
    PythonPackage(
        const std::string & package_with_version_,
        const std::string & requirements_text_,
        bool install_from_requirements_,
        PipInstallOptions install_options_);

    void parsePackageWithVersion();
};

using PythonPackages = std::vector<PythonPackage>;
}
