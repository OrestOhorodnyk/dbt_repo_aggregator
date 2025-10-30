#!/bin/bash

# Test script to verify compatibility of Airflow, Cosmos, and dbt versions
# This creates a clean virtual environment and tests basic imports and functionality

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.test_venv"
LOG_FILE="$SCRIPT_DIR/test_compatibility.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Library versions to test
AIRFLOW_VERSION="3.0.6"
COSMOS_VERSION="2.0.0"
DBT_CORE_VERSION="1.8.8"
DBT_POSTGRES_VERSION="1.8.2"

echo "========================================="
echo "Library Compatibility Test"
echo "========================================="
echo "Testing versions:"
echo "  - Airflow: $AIRFLOW_VERSION"
echo "  - astronomer-cosmos: $COSMOS_VERSION"
echo "  - dbt-core: $DBT_CORE_VERSION"
echo "  - dbt-postgres: $DBT_POSTGRES_VERSION"
echo "========================================="
echo ""

# Clean up any existing venv
if [ -d "$VENV_DIR" ]; then
    echo -e "${YELLOW}Removing existing virtual environment...${NC}"
    rm -rf "$VENV_DIR"
fi

# Create virtual environment
echo -e "${YELLOW}Creating virtual environment...${NC}"
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

# Upgrade pip and install uv for faster installation
echo -e "${YELLOW}Upgrading pip and installing uv...${NC}"
pip install --upgrade pip > /dev/null 2>&1

# Try to install uv, but continue if it fails
if command -v curl > /dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh > /dev/null 2>&1 || true
    export PATH="$HOME/.cargo/bin:$PATH" 2>/dev/null || true
fi

USE_UV=""
if command -v uv > /dev/null 2>&1; then
    USE_UV="uv pip"
    echo -e "${GREEN}Using uv for faster installation${NC}"
else
    USE_UV="pip"
    echo -e "${YELLOW}uv not available, using pip${NC}"
fi

# Install Airflow first (constraint files ensure compatible providers)
echo -e "${YELLOW}Installing Apache Airflow $AIRFLOW_VERSION...${NC}"
$USE_UV install "apache-airflow==$AIRFLOW_VERSION" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-$(python3 --version | grep -oE '[0-9]+\.[0-9]+' | head -1).txt" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to install Airflow${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Airflow installed${NC}"

# Install astronomer-cosmos
echo -e "${YELLOW}Installing astronomer-cosmos $COSMOS_VERSION...${NC}"
$USE_UV install "astronomer-cosmos==$COSMOS_VERSION" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to install astronomer-cosmos${NC}"
    exit 1
fi
echo -e "${GREEN}✅ astronomer-cosmos installed${NC}"

# Install dbt packages
echo -e "${YELLOW}Installing dbt-core $DBT_CORE_VERSION...${NC}"
$USE_UV install "dbt-core==$DBT_CORE_VERSION" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to install dbt-core${NC}"
    exit 1
fi
echo -e "${GREEN}✅ dbt-core installed${NC}"

echo -e "${YELLOW}Installing dbt-postgres $DBT_POSTGRES_VERSION...${NC}"
$USE_UV install "dbt-postgres==$DBT_POSTGRES_VERSION" >> "$LOG_FILE" 2>&1

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to install dbt-postgres${NC}"
    exit 1
fi
echo -e "${GREEN}✅ dbt-postgres installed${NC}"

echo ""
echo "========================================="
echo "Running Compatibility Tests"
echo "========================================="

# Create test script
cat > "$VENV_DIR/test_imports.py" << 'EOF'
"""Test imports and basic functionality"""

import sys

def test_airflow_import():
    """Test Airflow imports"""
    try:
        from airflow import __version__
        from airflow.models import DAG
        from airflow.operators.empty import EmptyOperator
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.operators.python import PythonOperator
        print(f"✅ Airflow {__version__} imports successful")
        return True
    except Exception as e:
        print(f"❌ Airflow import failed: {e}")
        return False

def test_cosmos_import():
    """Test Cosmos imports"""
    try:
        from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
        from cosmos.profiles import PostgresUserPasswordProfileMapping
        print("✅ Cosmos imports successful")
        return True
    except Exception as e:
        print(f"❌ Cosmos import failed: {e}")
        return False

def test_dbt_import():
    """Test dbt imports"""
    try:
        import dbt
        from dbt.adapters.postgres import PostgresAdapter
        print(f"✅ dbt imports successful (version: {dbt.__version__})")
        return True
    except Exception as e:
        print(f"❌ dbt import failed: {e}")
        return False

def test_version_compatibility():
    """Test version compatibility"""
    try:
        from airflow import __version__ as airflow_version
        import cosmos
        import dbt
        
        print(f"\n📦 Installed versions:")
        print(f"   Airflow: {airflow_version}")
        print(f"   Cosmos: {cosmos.__version__ if hasattr(cosmos, '__version__') else 'unknown'}")
        print(f"   dbt-core: {dbt.__version__}")
        
        # Check Airflow version compatibility
        major, minor = map(int, airflow_version.split('.')[:2])
        if (major == 2 and minor >= 3) or major >= 3:
            print("✅ Airflow version is compatible with Cosmos 2.0+ (requires 2.3+)")
            return True
        else:
            print(f"⚠️  Airflow version {airflow_version} may not be fully compatible")
            return False
    except Exception as e:
        print(f"❌ Version check failed: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality"""
    try:
        from cosmos import ProfileConfig, ProjectConfig
        from cosmos.profiles import PostgresUserPasswordProfileMapping
        
        # Try to create a profile config (this should work without actual DB connection)
        profile = ProfileConfig(
            profile_name="test",
            target_name="test",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="test",
                profile_args={"schema": "test"}
            ),
        )
        print("✅ Cosmos ProfileConfig creation successful")
        return True
    except Exception as e:
        print(f"❌ Cosmos functionality test failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing imports and compatibility...\n")
    
    results = []
    results.append(("Airflow Import", test_airflow_import()))
    results.append(("Cosmos Import", test_cosmos_import()))
    results.append(("dbt Import", test_dbt_import()))
    results.append(("Version Compatibility", test_version_compatibility()))
    results.append(("Basic Functionality", test_basic_functionality()))
    
    print("\n" + "="*40)
    print("Test Summary")
    print("="*40)
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
        if not result:
            all_passed = False
    
    print("="*40)
    
    if all_passed:
        print("\n🎉 All compatibility tests passed!")
        sys.exit(0)
    else:
        print("\n⚠️  Some tests failed. Check the output above.")
        sys.exit(1)
EOF

# Run tests
python "$VENV_DIR/test_imports.py"

TEST_RESULT=$?

echo ""
echo "========================================="
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}✅ All compatibility tests passed!${NC}"
    echo ""
    echo "You can now deploy with confidence."
else
    echo -e "${RED}❌ Some compatibility tests failed.${NC}"
    echo ""
    echo "Check the output above for details."
    echo "Full log available at: $LOG_FILE"
fi
echo "========================================="

# Cleanup option
echo ""
read -p "Do you want to keep the virtual environment for further testing? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Cleaning up virtual environment...${NC}"
    deactivate 2>/dev/null || true
    rm -rf "$VENV_DIR"
    echo -e "${GREEN}Cleanup complete${NC}"
else
    echo -e "${GREEN}Virtual environment kept at: $VENV_DIR${NC}"
    echo "To activate it, run: source $VENV_DIR/bin/activate"
fi

exit $TEST_RESULT

