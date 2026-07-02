#pragma once

/// Get number of CPU cores without hyper-threading.
/// The calculation respects possible cgroups limits.
unsigned getNumberOfPhysicalCPUCores();

/// proton: starts.
/// Get number of CPU cores with hyper-threading.
unsigned getNumberOfLogicalCPUCores();
/// proton: ends.
