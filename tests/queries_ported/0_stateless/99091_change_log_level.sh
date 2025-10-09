#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

BASE_URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"

# Helper functions
log_info() {
    echo "[INFO] $1"
}

log_success() {
    echo "ok"
}

log_warning() {
    echo "[WARNING] $1"
}

log_error() {
    echo "failed"
}

print_separator() {
    echo "=================================================="
}

# Execute SQL command with error handling
execute_sql() {
    local query="$1"
    local description="$2"
    local expect_failure="${3:-false}"
    
    if [[ "$expect_failure" == "true" ]]; then
        log_info "Testing expected failure: $description"
        if $CLICKHOUSE_CLIENT --query "$query" 2>/dev/null; then
            log_warning "Expected failure but command succeeded: $description"
            return 1
        else
            log_success
            return 0
        fi
    else
        log_info "$description"
        if $CLICKHOUSE_CLIENT --query "$query" 2>/dev/null; then
            log_success
            return 0
        else
            log_error
            return 1
        fi
    fi
}

# Execute REST API call with error handling
execute_rest() {
    local method="$1"
    local endpoint="$2"
    local data="$3"
    local description="$4"
    
    log_info "$description"
    
    if [[ "$method" == "GET" ]]; then
        if curl -s -X GET "${BASE_URL}${endpoint}" | jq '.' >/dev/null 2>&1; then
            log_success
            return 0
        else
            log_error
            return 1
        fi
    elif [[ "$method" == "POST" ]]; then
        if curl -s -X POST "${BASE_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -d "$data" | jq '.' >/dev/null 2>&1; then
            log_success
            return 0
        else
            log_error
            return 1
        fi
    fi
}

# Check existing loggers
check_existing_loggers() {
    log_info "Checking existing loggers..."
    print_separator
    
    # Just verify that SYSTEM SHOW LOGGERS works, don't print all names
    if $CLICKHOUSE_CLIENT --query "SYSTEM SHOW LOGGERS" >/dev/null 2>&1; then
        log_info "SYSTEM SHOW LOGGERS command works"
        log_success
    else
        log_info "SYSTEM SHOW LOGGERS command failed"
        log_error
    fi
    echo
}

# Test REST API endpoints
test_rest_api() {
    log_info "Testing REST API endpoints..."
    print_separator
    
    # Get current log levels
    execute_rest "GET" "/proton/v1/log_level" "" "Getting current log levels via REST API"
    echo
    
    # Set global log level to debug
    execute_rest "POST" "/proton/v1/log_level" '{"level": "debug"}' "Setting global log level to debug via REST API"
    echo
    
    # Set specific logger levels using known existing loggers
    execute_rest "POST" "/proton/v1/log_level" '{"level": "trace", "logger_name": "DiskLocal"}' "Setting DiskLocal logger to trace via REST API"
    echo
    
    execute_rest "POST" "/proton/v1/log_level" '{"level": "information", "name": "DNSResolver"}' "Setting DNSResolver logger to information via REST API"
    echo
    
    execute_rest "POST" "/proton/v1/log_level" '{"level": "warning", "logger_name": "MemoryTracker"}' "Setting MemoryTracker logger to warning via REST API"
    echo
}

# Test SQL commands
test_sql_commands() {
    print_separator
    
    # Set global log level via SQL
    execute_sql "SYSTEM SET LOG LEVEL information" "Setting global log level to information via SQL"
    
    # Set specific loggers via SQL using known existing loggers
    execute_sql "SYSTEM SET LOG LEVEL debug FOR 'DiskLocal'" "Setting DiskLocal logger to debug via SQL"
    
    execute_sql "SYSTEM SET LOG LEVEL trace FOR 'DNSResolver'" "Setting DNSResolver logger to trace via SQL"
    
    execute_sql "SYSTEM SET LOG LEVEL warning FOR 'MemoryTracker'" "Setting MemoryTracker logger to warning via SQL"
    
    # Test error case - invalid log level (expected to fail)
    execute_sql "SYSTEM SET LOG LEVEL invalid_level" "Testing invalid log level (expected to fail)" "true"
    
    # Test error case - non-existent logger (expected to fail)
    execute_sql "SYSTEM SET LOG LEVEL debug FOR 'NonExistentLogger'" "Testing non-existent logger (expected to fail)" "true"
}

# Test mixed scenarios
test_mixed_scenarios() {
    log_info "Testing mixed scenarios..."
    print_separator
    
    # Set via REST API, query via SQL
    execute_rest "POST" "/proton/v1/log_level" '{"level": "error", "logger_name": "DiskLocal"}' "Setting DiskLocal to error via REST API"
    
    # Get current levels again
    execute_rest "GET" "/proton/v1/log_level" "" "Getting updated log levels"
    echo
    
    # Reset to default via SQL
    execute_sql "SYSTEM SET LOG LEVEL information" "Resetting to default log level via SQL"
}

# Test various log levels
test_log_levels() {
    log_info "Testing all available log levels..."
    print_separator
    
    local levels=("trace" "debug" "information" "warning" "error" "fatal")
    
    for level in "${levels[@]}"; do
        execute_sql "SYSTEM SET LOG LEVEL $level" "Testing log level: $level"
        sleep 1
    done
}

# Test logger name patterns with existing loggers
test_logger_patterns() {
    log_info "Testing various logger name patterns with existing loggers..."
    print_separator
    
    # Test known existing loggers
    local loggers=(
        "DiskLocal"
        "DNSResolver"
        "MemoryTracker"
    )
    
    for logger in "${loggers[@]}"; do
        execute_sql "SYSTEM SET LOG LEVEL debug FOR '$logger'" "Testing existing logger: $logger"
        sleep 0.5
    done
    
    # Test some potentially non-existent loggers (expected to fail)
    local potentially_missing_loggers=(
        "non-existent-loggers"
        "BackgroundSchedulePool"
    )
    
    log_info "Testing potentially non-existent loggers (expected to fail)..."
    for logger in "${potentially_missing_loggers[@]}"; do
        execute_sql "SYSTEM SET LOG LEVEL debug FOR '$logger'" "Testing potentially missing logger: $logger" "true"
        sleep 0.5
    done
}

# Test comprehensive logger discovery
test_logger_discovery() {
    log_info "Testing logger discovery and validation..."
    print_separator
    
    # First, get logger count without printing all names
    log_info "Discovering available loggers..."
    LOGGER_COUNT=$(mktemp)
    $CLICKHOUSE_CLIENT --query "SYSTEM SHOW LOGGERS" 2>/dev/null | grep -c "." > "$LOGGER_COUNT"
    
    if [[ -s "$LOGGER_COUNT" ]] && [[ $(cat "$LOGGER_COUNT") -gt 0 ]]; then
        
        # Test setting log level for a few common loggers without listing all
        local test_loggers=("MemoryTracker" "DiskLocal" "DNSResolver")
        local count=0
        for logger in "${test_loggers[@]}"; do
            if [[ $count -lt 3 ]]; then
                execute_sql "SYSTEM SET LOG LEVEL debug FOR '$logger'" "Testing common logger: $logger"
                ((count++))
            fi
        done
    else
        log_info "Could not discover loggers or no loggers found"
        log_error
    fi
    
    # Clean up
    rm -f "$LOGGER_COUNT"
}

# Main execution
main() {
    log_success
    
    # Run all tests
    check_existing_loggers
    echo
    test_rest_api
    echo
    test_sql_commands
    echo
    test_mixed_scenarios
    echo
    test_log_levels
    echo
    test_logger_patterns
    echo
    test_logger_discovery
    
    print_separator
    log_info "All tests completed"
    log_success
    
    # Final status check
    log_info "Final log level status:"
    execute_rest "GET" "/proton/v1/log_level" "" "Final status check"

    log_info "Setting log level to trace for stateless tests"
    $CLICKHOUSE_CLIENT --query "SYSTEM SET LOG LEVEL trace"
}

# Run the main function
main "$@"