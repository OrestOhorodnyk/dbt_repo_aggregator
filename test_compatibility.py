#!/usr/bin/env python3
"""
Python-based compatibility test script
Alternative to bash script, can be run directly with: python test_compatibility.py
"""

import subprocess
import sys
import os
from pathlib import Path

# Library versions to test
VERSIONS = {
    "airflow": "3.0.6",
    "cosmos": "2.0.0",
    "dbt-core": "1.8.8",
    "dbt-postgres": "1.8.2",
}

def run_command(cmd, check=True, capture_output=False):
    """Run a command and return the result"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        if check:
            print(f"❌ Command failed: {cmd}")
            print(f"Error: {e}")
            sys.exit(1)
        return None

def install_packages():
    """Install packages in the current environment"""
    print("=" * 50)
    print("Installing packages for compatibility testing...")
    print("=" * 50)
    
    # Check if uv is available
    use_uv = run_command("command -v uv", check=False, capture_output=True)
    installer = "uv pip" if use_uv and use_uv.returncode == 0 else "pip"
    
    if installer == "uv pip":
        print("✅ Using uv for faster installation")
    else:
        print("ℹ️  Using pip (consider installing uv for faster builds)")
    
    # Upgrade pip first
    print(f"\n📦 Upgrading pip...")
    run_command(f"{installer} install --upgrade pip", capture_output=True)
    
    # Install Airflow with constraints
    print(f"\n📦 Installing Apache Airflow {VERSIONS['airflow']}...")
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    constraint_url = f"https://raw.githubusercontent.com/apache/airflow/constraints-{VERSIONS['airflow']}/constraints-{python_version}.txt"
    
    cmd = f"{installer} install apache-airflow=={VERSIONS['airflow']} --constraint {constraint_url}"
    run_command(cmd, capture_output=True)
    print("✅ Airflow installed")
    
    # Install cosmos
    print(f"\n📦 Installing astronomer-cosmos {VERSIONS['cosmos']}...")
    run_command(f"{installer} install astronomer-cosmos=={VERSIONS['cosmos']}", capture_output=True)
    print("✅ astronomer-cosmos installed")
    
    # Install dbt packages
    print(f"\n📦 Installing dbt-core {VERSIONS['dbt-core']}...")
    run_command(f"{installer} install dbt-core=={VERSIONS['dbt-core']}", capture_output=True)
    print("✅ dbt-core installed")
    
    print(f"\n📦 Installing dbt-postgres {VERSIONS['dbt-postgres']}...")
    run_command(f"{installer} install dbt-postgres=={VERSIONS['dbt-postgres']}", capture_output=True)
    print("✅ dbt-postgres installed")

def test_imports():
    """Test all imports"""
    print("\n" + "=" * 50)
    print("Running Compatibility Tests")
    print("=" * 50 + "\n")
    
    results = []
    
    # Test Airflow imports
    try:
        from airflow import __version__ as airflow_version
        from airflow.models import DAG
        from airflow.operators.empty import EmptyOperator
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.operators.python import PythonOperator
        print(f"✅ Airflow {airflow_version} imports successful")
        results.append(("Airflow Import", True, airflow_version))
    except Exception as e:
        print(f"❌ Airflow import failed: {e}")
        results.append(("Airflow Import", False, str(e)))
    
    # Test Cosmos imports
    try:
        from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
        from cosmos.profiles import PostgresUserPasswordProfileMapping
        cosmos_version = getattr(__import__('cosmos'), '__version__', 'unknown')
        print("✅ Cosmos imports successful")
        results.append(("Cosmos Import", True, cosmos_version))
    except Exception as e:
        print(f"❌ Cosmos import failed: {e}")
        results.append(("Cosmos Import", False, str(e)))
    
    # Test dbt imports
    try:
        import dbt
        from dbt.adapters.postgres import PostgresAdapter
        print(f"✅ dbt imports successful (version: {dbt.__version__})")
        results.append(("dbt Import", True, dbt.__version__))
    except Exception as e:
        print(f"❌ dbt import failed: {e}")
        results.append(("dbt Import", False, str(e)))
    
    # Test version compatibility
    try:
        from airflow import __version__ as airflow_version
        major, minor = map(int, airflow_version.split('.')[:2])
        if (major == 2 and minor >= 3) or major >= 3:
            print("✅ Airflow version is compatible with Cosmos 2.0+ (requires 2.3+)")
            results.append(("Version Compatibility", True, airflow_version))
        else:
            print(f"⚠️  Airflow version {airflow_version} may not be fully compatible")
            results.append(("Version Compatibility", False, airflow_version))
    except Exception as e:
        print(f"❌ Version check failed: {e}")
        results.append(("Version Compatibility", False, str(e)))
    
    # Test basic functionality
    try:
        from cosmos import ProfileConfig
        from cosmos.profiles import PostgresUserPasswordProfileMapping
        
        profile = ProfileConfig(
            profile_name="test",
            target_name="test",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="test",
                profile_args={"schema": "test"}
            ),
        )
        print("✅ Cosmos ProfileConfig creation successful")
        results.append(("Basic Functionality", True, "OK"))
    except Exception as e:
        print(f"❌ Cosmos functionality test failed: {e}")
        results.append(("Basic Functionality", False, str(e)))
    
    return results

def print_summary(results):
    """Print test summary"""
    print("\n" + "=" * 50)
    print("Test Summary")
    print("=" * 50)
    
    all_passed = True
    for test_name, passed, info in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
        if info and passed:
            print(f"         Version: {info}")
        if not passed:
            all_passed = False
    
    print("=" * 50)
    
    if all_passed:
        print("\n🎉 All compatibility tests passed!")
        print("\n📦 Installed versions:")
        for test_name, passed, info in results:
            if passed and info and test_name in ["Airflow Import", "Cosmos Import", "dbt Import"]:
                print(f"   {test_name.replace(' Import', '')}: {info}")
    else:
        print("\n⚠️  Some tests failed. Check the output above.")
    
    return all_passed

def main():
    """Main function"""
    print("\n" + "=" * 50)
    print("Library Compatibility Test")
    print("=" * 50)
    print("Testing versions:")
    for lib, version in VERSIONS.items():
        print(f"  - {lib}: {version}")
    print("=" * 50 + "\n")
    
    # Check if we're in a virtual environment
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    
    if not in_venv:
        print("⚠️  Warning: Not running in a virtual environment!")
        print("It's recommended to run this in a venv to avoid conflicts.")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting. Create a venv first: python3 -m venv .test_venv && source .test_venv/bin/activate")
            sys.exit(0)
    
    try:
        install_packages()
        results = test_imports()
        all_passed = print_summary(results)
        sys.exit(0 if all_passed else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

