#pragma once

/// Get number of CPU cores without hyper-threading.
unsigned getNumberOfPhysicalCPUCores();

/// proton: starts.
/// Get number of CPU cores with hyper-threading.
unsigned getNumberOfLogicalCPUCores();
/// proton: ends.
