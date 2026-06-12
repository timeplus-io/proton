#include <Interpreters/PythonRequirementsReconciler.h>

#if USE_PYTHON_UDF && USE_AWS_S3

#include <gtest/gtest.h>

#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/MapConfiguration.h>

#include <chrono>
#include <filesystem>
#include <string>

using DB::PythonRequirementsReconciler;

namespace
{
std::filesystem::path makeTestRoot()
{
    namespace fs = std::filesystem;
    const auto unique_suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    auto test_root = fs::absolute(fs::path("tmp") / ("gtest_python_requirements_" + unique_suffix));
    fs::create_directories(test_root);
    return test_root;
}
}

TEST(PythonRequirementsReconciler, ContentDigestIsStableAndContentSensitive)
{
    const std::string text = "requests==2.31.0\nnumpy==1.26.4\n";

    const auto digest = PythonRequirementsReconciler::contentDigest(text);
    EXPECT_FALSE(digest.empty());
    EXPECT_EQ(digest, PythonRequirementsReconciler::contentDigest(text));

    EXPECT_NE(digest, PythonRequirementsReconciler::contentDigest("requests==2.31.0\nnumpy==1.26.5\n"));
    EXPECT_NE(digest, PythonRequirementsReconciler::contentDigest(""));
}

TEST(PythonRequirementsReconciler, MarkerRoundTrip)
{
    namespace fs = std::filesystem;
    const auto test_root = makeTestRoot();
    const auto user_base = (test_root / "userbase").string();

    /// No marker yet — first boot on an ephemeral node must decide to apply.
    EXPECT_EQ(PythonRequirementsReconciler::readMarker(user_base), "");

    const auto digest = PythonRequirementsReconciler::contentDigest("requests==2.31.0\n");
    /// writeMarker creates the user base directory if needed.
    ASSERT_NO_THROW(PythonRequirementsReconciler::writeMarker(user_base, digest));
    EXPECT_EQ(PythonRequirementsReconciler::readMarker(user_base), digest);

    /// Changed requirements content produces a different digest, so a reconcile is due again.
    const auto new_digest = PythonRequirementsReconciler::contentDigest("requests==2.32.0\n");
    EXPECT_NE(PythonRequirementsReconciler::readMarker(user_base), new_digest);

    ASSERT_NO_THROW(PythonRequirementsReconciler::writeMarker(user_base, new_digest));
    EXPECT_EQ(PythonRequirementsReconciler::readMarker(user_base), new_digest);

    std::error_code error_code;
    fs::remove_all(test_root, error_code);
}

TEST(PythonRequirementsReconciler, LoadConfigAbsentOrEmpty)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);

    EXPECT_FALSE(PythonRequirementsReconciler::loadConfig(*config).has_value());

    config->setString("python_requirements.url", "");
    EXPECT_FALSE(PythonRequirementsReconciler::loadConfig(*config).has_value());
}

TEST(PythonRequirementsReconciler, ReconcileOnceSkipsWhenDigestAlreadyApplied)
{
    namespace fs = std::filesystem;
    const auto test_root = makeTestRoot();
    const auto user_base = (test_root / "userbase").string();

    const std::string requirements = "requests==2.31.0\n";
    PythonRequirementsReconciler::writeMarker(user_base, PythonRequirementsReconciler::contentDigest(requirements));

    /// With the marker matching the fetched content, reconcileOnce must finish without touching
    /// the package manager (the context is null, so reaching the install path would crash).
    PythonRequirementsReconciler::Config config;
    config.url = "stub://requirements";
    PythonRequirementsReconciler reconciler(config, nullptr, [&] { return requirements; });
    EXPECT_TRUE(reconciler.reconcileOnce(user_base, /* attempt = */ 1));

    std::error_code error_code;
    fs::remove_all(test_root, error_code);
}

TEST(PythonRequirementsReconciler, ReconcileOnceFinishesOnEmptyRequirements)
{
    namespace fs = std::filesystem;
    const auto test_root = makeTestRoot();
    const auto user_base = (test_root / "userbase").string();

    /// Comment-only content parses to zero requirements: nothing to install, reconcile is done.
    PythonRequirementsReconciler::Config config;
    config.url = "stub://requirements";
    PythonRequirementsReconciler reconciler(config, nullptr, [] { return std::string("# nothing here\n\n"); });
    EXPECT_TRUE(reconciler.reconcileOnce(user_base, /* attempt = */ 1));
    /// No marker is written for empty content.
    EXPECT_EQ(PythonRequirementsReconciler::readMarker(user_base), "");

    std::error_code error_code;
    fs::remove_all(test_root, error_code);
}

TEST(PythonRequirementsReconciler, ReconcileOnceRejectsOversizeContent)
{
    namespace fs = std::filesystem;
    const auto test_root = makeTestRoot();
    const auto user_base = (test_root / "userbase").string();

    PythonRequirementsReconciler::Config config;
    config.url = "stub://requirements";
    PythonRequirementsReconciler reconciler(
        config, nullptr, [] { return std::string(PythonRequirementsReconciler::MAX_REQUIREMENTS_BYTES + 1, '#'); });
    EXPECT_THROW(reconciler.reconcileOnce(user_base, /* attempt = */ 1), DB::Exception);

    std::error_code error_code;
    fs::remove_all(test_root, error_code);
}

TEST(PythonRequirementsReconciler, LoadConfigParsesAllFields)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
    config->setString("python_requirements.url", "https://bucket.s3.us-west-2.amazonaws.com/proton/requirements.txt");
    config->setString("python_requirements.index_url", "https://pypi.org/simple");
    config->setString("python_requirements.extra_index_url", "https://mirror-a.example.com/simple");
    config->setString("python_requirements.extra_index_url[1]", "https://mirror-b.example.com/simple");

    const auto parsed = PythonRequirementsReconciler::loadConfig(*config);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->url, "https://bucket.s3.us-west-2.amazonaws.com/proton/requirements.txt");
    EXPECT_EQ(parsed->index_url, "https://pypi.org/simple");
    ASSERT_EQ(parsed->extra_index_urls.size(), 2u);
    EXPECT_EQ(parsed->extra_index_urls[0], "https://mirror-a.example.com/simple");
    EXPECT_EQ(parsed->extra_index_urls[1], "https://mirror-b.example.com/simple");
    EXPECT_EQ(parsed->poll_interval_sec, PythonRequirementsReconciler::DEFAULT_POLL_INTERVAL_SEC);
}

TEST(PythonRequirementsReconciler, LoadConfigParsesPollInterval)
{
    Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
    config->setString("python_requirements.url", "https://bucket.s3.us-west-2.amazonaws.com/proton/requirements.txt");

    config->setString("python_requirements.poll_interval_sec", "600");
    EXPECT_EQ(PythonRequirementsReconciler::loadConfig(*config)->poll_interval_sec, 600u);

    /// 0 means startup-only (polling disabled) and must not be clamped up.
    config->setString("python_requirements.poll_interval_sec", "0");
    EXPECT_EQ(PythonRequirementsReconciler::loadConfig(*config)->poll_interval_sec, 0u);

    /// Tiny non-zero intervals are clamped to the minimum to avoid hammering S3.
    config->setString("python_requirements.poll_interval_sec", "1");
    EXPECT_EQ(PythonRequirementsReconciler::loadConfig(*config)->poll_interval_sec, PythonRequirementsReconciler::MIN_POLL_INTERVAL_SEC);
}

#endif
