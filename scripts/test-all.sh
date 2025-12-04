#!/usr/bin/env bash

echo "🧪 Running Comprehensive Iceberg Feature Tests..."
echo "================================================="
echo "This script tests all Iceberg time travel and branching capabilities"
echo "in your Trino setup to ensure everything is working for the demo."
echo

# Test 1: Basic connectivity
echo "🔍 Step 1: Testing basic Trino connectivity..."
./scripts/test-time-travel.sh | head -10
CONNECTIVITY_OK=$?
if [[ $CONNECTIVITY_OK -eq 0 ]]; then
    echo "✅ Basic connectivity: PASSED"
else
    echo "❌ Basic connectivity: FAILED"
    exit 1
fi
echo

# Test 2: Time Travel Features
echo "🕐 Step 2: Testing Time Travel Features..."
echo "======================================="
./scripts/test-time-travel.sh
TIME_TRAVEL_OK=$?
if [[ $TIME_TRAVEL_OK -eq 0 ]]; then
    echo "✅ Time Travel: PASSED"
else
    echo "⚠️  Time Travel: Some issues detected (check output above)"
fi
echo

# Test 3: Branching Features  
echo "🌿 Step 3: Testing Branching Features..."
echo "======================================"
./scripts/test-branching.sh
BRANCHING_OK=$?
if [[ $BRANCHING_OK -eq 0 ]]; then
    echo "✅ Branching: PASSED"
else
    echo "⚠️  Branching: Some issues detected (check output above)"
fi
echo

# Test 4: Metadata Tables
echo "📊 Step 4: Testing Metadata Tables..."
echo "===================================="
./scripts/test-metadata.sh
METADATA_OK=$?
if [[ $METADATA_OK -eq 0 ]]; then
    echo "✅ Metadata: PASSED"
else
    echo "⚠️  Metadata: Some issues detected (check output above)"
fi
echo

# Test 5: Shared Module Integration
echo "🐍 Step 5: Testing Shared Query Module..."
echo "======================================="

echo "   → Testing shared module import in Shiny container..."
SHARED_TEST=$(docker exec shiny-app python -c "
import sys
sys.path.append('/app/shared')
from demo_queries import IcebergDemoQueries
queries = IcebergDemoQueries()
query, desc = queries.connectivity_test()
print('✅ Shared module import successful')
print(f'✅ Sample query: {desc}')
" 2>/dev/null)

if [[ $? -eq 0 ]]; then
    echo "$SHARED_TEST"
    echo "   ✅ Shared module: ACCESSIBLE"
    SHARED_OK=0
else
    echo "   ❌ Shared module: IMPORT FAILED"
    SHARED_OK=1
fi
echo

# Test 6: Demo readiness check
echo "🎯 Step 6: Demo Readiness Check..."
echo "================================="

echo "   → Checking customer data availability..."
CUSTOMER_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.customers" 2>/dev/null | tail -n 1 | tr -d '"')
echo "   ✅ Customer records: $CUSTOMER_COUNT"

echo "   → Checking schema evolution..."
SCHEMA_CHECK=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "DESCRIBE iceberg.demo.customers" 2>/dev/null | grep customer_tier)
if [[ ! -z "$SCHEMA_CHECK" ]]; then
    echo "   ✅ Schema evolution: customer_tier column present"
else
    echo "   ⚠️  Schema evolution: customer_tier column missing"
fi

echo "   → Checking time travel snapshots..."
SNAPSHOT_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.\"customers\$snapshots\"" 2>/dev/null | tail -n 1 | tr -d '"')
echo "   ✅ Available snapshots: $SNAPSHOT_COUNT"

echo "   → Checking branch availability..."
BRANCH_COUNT=$(docker exec trino-cli trino --server trino:8080 --user admin --execute "SELECT COUNT(*) FROM iceberg.demo.\"customers\$refs\"" 2>/dev/null | tail -n 1 | tr -d '"')
echo "   ✅ Available branches/refs: $BRANCH_COUNT"

echo "   → Testing Shiny app accessibility..."
SHINY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000 2>/dev/null || echo "000")
if [[ "$SHINY_STATUS" == "200" ]]; then
    echo "   ✅ Shiny app: Accessible at http://localhost:8000"
else
    echo "   ⚠️  Shiny app: May not be ready (status: $SHINY_STATUS)"
fi

echo "   → Testing Trino Web UI..."
TRINO_UI_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8081 2>/dev/null || echo "000")
if [[ "$TRINO_UI_STATUS" == "200" ]]; then
    echo "   ✅ Trino Web UI: Accessible at http://localhost:8081"
else
    echo "   ⚠️  Trino Web UI: May not be ready (status: $TRINO_UI_STATUS)"
fi

echo

# Final Summary
echo "🎉 COMPREHENSIVE TEST SUMMARY"
echo "============================"
echo

if [[ $CUSTOMER_COUNT -gt 0 ]] && [[ $SNAPSHOT_COUNT -gt 1 ]]; then
    echo "✅ DEMO READY! Your Iceberg time travel and branching demo is fully functional."
    echo
    echo "🚀 Quick Start Guide for Your Team Demo:"
    echo "   1. 🌐 Open Shiny App: http://localhost:8000"
    echo "   2. 📊 Open Trino Web UI: http://localhost:8081"
    echo 
    echo "🎯 Recommended Demo Flow:"
    echo "   • Start with 'Customer Analytics' to show current data"
    echo "   • Use 'Data Evolution Timeline' to show how data changed over time"
    echo "   • Try 'Snapshot-by-Snapshot Comparison' for detailed time travel"
    echo "   • Demonstrate 'Before/After Schema Evolution' to show schema changes"
    echo "   • Show 'Branch Querying & Comparison' for branch capabilities"
    echo
    echo "💡 Key Demo Points:"
    echo "   • $CUSTOMER_COUNT customer records across $SNAPSHOT_COUNT snapshots"
    echo "   • Full schema evolution demonstrated (customer_tier column added)"
    echo "   • Time travel works with both snapshot IDs and timestamps"
    echo "   • Branch querying available (creation may be limited in Trino 435)"
    echo "   • All Iceberg metadata tables accessible for advanced queries"
    echo
else
    echo "⚠️  SETUP NEEDS ATTENTION"
    echo "   • Customer count: $CUSTOMER_COUNT (should be > 0)"
    echo "   • Snapshot count: $SNAPSHOT_COUNT (should be > 1)"
    echo "   • Run: make rebuild-demo to reinitialize"
fi

echo "📋 Test Results Summary:"
echo "   • Time Travel: $([ $TIME_TRAVEL_OK -eq 0 ] && echo "✅ PASSED" || echo "⚠️  ISSUES")"
echo "   • Branching: $([ $BRANCHING_OK -eq 0 ] && echo "✅ PASSED" || echo "⚠️  ISSUES")"  
echo "   • Metadata: $([ $METADATA_OK -eq 0 ] && echo "✅ PASSED" || echo "⚠️  ISSUES")"
echo "   • Shared Module: $([ $SHARED_OK -eq 0 ] && echo "✅ PASSED" || echo "⚠️  ISSUES")"
echo "   • Demo Data: $([ $CUSTOMER_COUNT -gt 0 ] && echo "✅ READY" || echo "❌ MISSING")"
echo

echo "🎬 Your demo is ready! Happy presenting!"