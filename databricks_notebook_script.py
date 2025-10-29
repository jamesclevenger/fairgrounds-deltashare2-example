# Databricks Notebook Script for Delta Sharing
# Copy and paste this entire script into a single Databricks notebook cell

# ====================================================================
# STEP 1: Install and Import Required Libraries
# ====================================================================

# Install delta-sharing if not already available
%pip install delta-sharing

# Import required libraries
import delta_sharing
import pandas as pd
import json
import os
from datetime import datetime
import traceback

print("✅ Libraries imported successfully")
print(f"Delta Sharing version: {delta_sharing.__version__}")

# ====================================================================
# STEP 2: Configuration - Your Delta Share Server Details
# ====================================================================

# Your Azure Delta Share deployment configuration
DELTA_SHARE_CONFIG = {
    "shareCredentialsVersion": 1,
    "endpoint": "https://fairgrounds-deltashare-production-endpoint-XXXXXXXX.z01.azurefd.net",
    "bearerToken": "9b633f0dc26742388aeadda89b88e5f0"
}

# Profile file path in DBFS
PROFILE_PATH = "/dbfs/FileStore/fairgrounds_deltashare_profile.json"

print("🔧 Configuration loaded")
print(f"Endpoint: {DELTA_SHARE_CONFIG['endpoint']}")
print(f"Bearer Token: {'*' * len(DELTA_SHARE_CONFIG['bearerToken'])}")

# ====================================================================
# STEP 3: Create Delta Share Profile File
# ====================================================================

def create_profile_file():
    """Create the Delta Share profile file in DBFS"""
    try:
        # Write profile to DBFS
        dbutils.fs.put(
            PROFILE_PATH.replace('/dbfs', ''), 
            json.dumps(DELTA_SHARE_CONFIG, indent=2), 
            overwrite=True
        )
        print(f"✅ Profile file created at: {PROFILE_PATH}")
        return True
    except Exception as e:
        print(f"❌ Error creating profile file: {e}")
        return False

# Create the profile file
if create_profile_file():
    print("📁 Delta Share profile ready")

# ====================================================================
# STEP 4: Initialize Delta Share Client
# ====================================================================

def test_connection():
    """Test connection to Delta Share server"""
    try:
        client = delta_sharing.SharingClient(PROFILE_PATH)
        shares = client.list_shares()
        print(f"✅ Connection successful! Found {len(shares)} shares")
        return client, shares
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔍 Troubleshooting tips:")
        print("1. Check if Delta Share server is running")
        print("2. Verify bearer token is correct")
        print("3. Ensure network connectivity to Azure endpoint")
        return None, None

# Test connection and get client
client, shares = test_connection()

# ====================================================================
# STEP 5: Data Discovery Functions
# ====================================================================

def discover_all_tables(client):
    """Discover all tables across all shares and schemas"""
    all_tables = []
    
    try:
        shares = client.list_shares()
        print(f"\n📊 Discovering tables in {len(shares)} shares...")
        
        for share in shares:
            share_name = share.name
            print(f"\n🔍 Share: {share_name}")
            
            try:
                schemas = client.list_schemas(share)
                print(f"   Found {len(schemas)} schemas")
                
                for schema in schemas:
                    schema_name = schema.name
                    print(f"   📁 Schema: {schema_name}")
                    
                    try:
                        tables = client.list_tables(schema)
                        print(f"      Found {len(tables)} tables")
                        
                        for table in tables:
                            table_info = {
                                'share': share_name,
                                'schema': schema_name,
                                'table': table.name,
                                'url': f"{PROFILE_PATH}#{share_name}.{schema_name}.{table.name}"
                            }
                            all_tables.append(table_info)
                            print(f"      📋 Table: {table.name}")
                            
                    except Exception as e:
                        print(f"      ❌ Error listing tables in {schema_name}: {e}")
                        
            except Exception as e:
                print(f"   ❌ Error listing schemas in {share_name}: {e}")
                
    except Exception as e:
        print(f"❌ Error listing shares: {e}")
    
    return all_tables

# Discover all available tables
if client:
    all_tables = discover_all_tables(client)
    print(f"\n🎯 Total tables discovered: {len(all_tables)}")
else:
    all_tables = []

# ====================================================================
# STEP 6: Data Loading and Display Functions
# ====================================================================

def load_and_display_table(table_info, sample_rows=10, show_full=False):
    """Load and display a Delta Share table as DataFrame"""
    
    table_name = f"{table_info['share']}.{table_info['schema']}.{table_info['table']}"
    print(f"\n{'='*60}")
    print(f"📊 TABLE: {table_name}")
    print(f"{'='*60}")
    
    try:
        # Load sample data first
        print(f"Loading sample data ({sample_rows} rows)...")
        sample_df = delta_sharing.load_as_pandas(table_info['url'], limit=sample_rows)
        
        print(f"✅ Sample loaded successfully!")
        print(f"📏 Sample shape: {sample_df.shape}")
        print(f"🏗️  Columns: {list(sample_df.columns)}")
        
        # Display sample data
        print(f"\n📋 Sample Data (first {min(sample_rows, len(sample_df))} rows):")
        display(sample_df)
        
        # Show data types
        print(f"\n🔧 Data Types:")
        print(sample_df.dtypes)
        
        # Load full table if requested and not too large
        if show_full:
            try:
                print(f"\n📥 Loading full table...")
                full_df = delta_sharing.load_as_pandas(table_info['url'])
                print(f"✅ Full table loaded!")
                print(f"📏 Full shape: {full_df.shape}")
                
                # Basic statistics for numeric columns
                numeric_cols = full_df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    print(f"\n📈 Numeric Summary:")
                    display(full_df[numeric_cols].describe())
                
                return full_df
            except Exception as e:
                print(f"⚠️  Could not load full table: {e}")
                print("📋 Returning sample data instead")
                return sample_df
        
        return sample_df
        
    except Exception as e:
        print(f"❌ Error loading table {table_name}: {e}")
        print(f"🔍 Error details: {traceback.format_exc()}")
        return None

def load_all_tables(all_tables, sample_rows=5):
    """Load all discovered tables as DataFrames"""
    loaded_tables = {}
    
    print(f"\n🚀 Loading all {len(all_tables)} tables...")
    print(f"📊 Sample size: {sample_rows} rows per table")
    
    for i, table_info in enumerate(all_tables, 1):
        table_key = f"{table_info['share']}.{table_info['schema']}.{table_info['table']}"
        print(f"\n[{i}/{len(all_tables)}] Processing: {table_key}")
        
        df = load_and_display_table(table_info, sample_rows=sample_rows)
        if df is not None:
            loaded_tables[table_key] = df
            print(f"✅ Stored in loaded_tables['{table_key}']")
        else:
            print(f"❌ Failed to load {table_key}")
    
    return loaded_tables

# ====================================================================
# STEP 7: Execute Data Loading
# ====================================================================

if all_tables:
    print(f"\n🎬 Starting data loading process...")
    print(f"📊 Will load {len(all_tables)} tables with sample data")
    
    # Load all tables with small samples first
    loaded_tables = load_all_tables(all_tables, sample_rows=5)
    
    print(f"\n🎉 DATA LOADING COMPLETE!")
    print(f"✅ Successfully loaded: {len(loaded_tables)} tables")
    print(f"❌ Failed to load: {len(all_tables) - len(loaded_tables)} tables")
    
    # Summary of loaded tables
    print(f"\n📋 LOADED TABLES SUMMARY:")
    print("-" * 60)
    for table_name, df in loaded_tables.items():
        print(f"📊 {table_name}: {df.shape[0]} rows × {df.shape[1]} columns")
    
else:
    print("❌ No tables discovered. Check your Delta Share server connection.")

# ====================================================================
# STEP 8: Utility Functions for Data Exploration
# ====================================================================

def explore_table(table_name):
    """Detailed exploration of a specific table"""
    if table_name not in loaded_tables:
        print(f"❌ Table '{table_name}' not found in loaded_tables")
        print(f"Available tables: {list(loaded_tables.keys())}")
        return
    
    df = loaded_tables[table_name]
    print(f"\n🔍 DETAILED EXPLORATION: {table_name}")
    print("=" * 60)
    
    # Basic info
    print(f"📏 Shape: {df.shape}")
    print(f"🧮 Memory usage: {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
    
    # Column info
    print(f"\n📋 Columns ({len(df.columns)}):")
    for i, col in enumerate(df.columns):
        print(f"  {i+1:2d}. {col} ({df[col].dtype})")
    
    # Missing values
    missing = df.isnull().sum()
    if missing.any():
        print(f"\n❓ Missing Values:")
        for col, count in missing[missing > 0].items():
            print(f"  {col}: {count} ({count/len(df)*100:.1f}%)")
    else:
        print(f"\n✅ No missing values found")
    
    # Show sample data
    print(f"\n📊 Sample Data:")
    display(df.head(10))
    
    return df

def compare_tables():
    """Compare all loaded tables"""
    if not loaded_tables:
        print("❌ No tables loaded for comparison")
        return
    
    print(f"\n📊 TABLE COMPARISON")
    print("=" * 60)
    
    comparison_data = []
    for table_name, df in loaded_tables.items():
        comparison_data.append({
            'Table': table_name,
            'Rows': df.shape[0],
            'Columns': df.shape[1],
            'Memory (KB)': round(df.memory_usage(deep=True).sum() / 1024, 1),
            'Missing Values': df.isnull().sum().sum()
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    display(comparison_df)
    
    return comparison_df

def load_full_table(table_name):
    """Load the complete data for a specific table"""
    # Find the table info
    table_info = None
    for t in all_tables:
        if f"{t['share']}.{t['schema']}.{t['table']}" == table_name:
            table_info = t
            break
    
    if not table_info:
        print(f"❌ Table '{table_name}' not found")
        return None
    
    print(f"📥 Loading full table: {table_name}")
    try:
        full_df = delta_sharing.load_as_pandas(table_info['url'])
        print(f"✅ Full table loaded: {full_df.shape}")
        loaded_tables[table_name] = full_df  # Update the stored version
        return full_df
    except Exception as e:
        print(f"❌ Error loading full table: {e}")
        return None

# ====================================================================
# STEP 9: Display Final Results and Usage Instructions
# ====================================================================

print(f"\n" + "="*80)
print(f"🎉 DELTA SHARING NOTEBOOK SETUP COMPLETE!")
print(f"="*80)

if loaded_tables:
    print(f"\n✅ Successfully connected to your Delta Share server!")
    print(f"📊 Loaded {len(loaded_tables)} tables as pandas DataFrames")
    
    print(f"\n📋 AVAILABLE TABLES:")
    for table_name in loaded_tables.keys():
        print(f"   📊 {table_name}")
    
    print(f"\n🔧 USAGE EXAMPLES:")
    print(f"   # Access a specific table:")
    first_table = list(loaded_tables.keys())[0]
    print(f"   df = loaded_tables['{first_table}']")
    print(f"   display(df)")
    print(f"")
    print(f"   # Explore a table in detail:")
    print(f"   explore_table('{first_table}')")
    print(f"")
    print(f"   # Compare all tables:")
    print(f"   compare_tables()")
    print(f"")
    print(f"   # Load full data for a table:")
    print(f"   full_df = load_full_table('{first_table}')")
    
    print(f"\n🔗 CONNECTION INFO:")
    print(f"   Server: {DELTA_SHARE_CONFIG['endpoint']}")
    print(f"   Profile: {PROFILE_PATH}")
    print(f"   Total Shares: {len(shares) if shares else 0}")
    print(f"   Total Tables: {len(all_tables)}")
    
else:
    print(f"\n❌ No tables were successfully loaded.")
    print(f"🔍 Troubleshooting steps:")
    print(f"   1. Check if your Delta Share server is running")
    print(f"   2. Verify the bearer token is correct")
    print(f"   3. Test network connectivity to: {DELTA_SHARE_CONFIG['endpoint']}")
    print(f"   4. Check server logs for errors")

print(f"\n💡 TIP: All functions and data are now available in this notebook!")
print(f"📝 Variables created:")
print(f"   - client: Delta Share client object")
print(f"   - all_tables: List of all discovered tables")
print(f"   - loaded_tables: Dictionary of loaded DataFrames")
print(f"   - Functions: explore_table(), compare_tables(), load_full_table()")

# ====================================================================
# END OF SCRIPT
# ====================================================================