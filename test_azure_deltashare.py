#!/usr/bin/env python3
"""
Test script to connect to Azure-deployed Delta Share server.
This script tests the deployed Azure Container Instances.
"""

import os
import sys
import requests
import json

# Azure deployment configuration
AZURE_DELTASHARE_URL = "http://fairgrounds-deltashare-development-deltashare.eastus.azurecontainer.io:8080"

def get_bearer_token():
    """Get bearer token from environment or prompt"""
    token = os.getenv('DELTA_SHARING_BEARER_TOKEN')
    if not token:
        token = input("Enter your Delta Sharing Bearer Token: ").strip()
    return token

def test_azure_deltashare():
    """Test the Azure-deployed Delta Share server"""
    
    print("=== Azure Delta Share Deployment Test ===\n")
    
    # Get bearer token
    bearer_token = get_bearer_token()
    if not bearer_token:
        print("❌ Bearer token required")
        return False
    
    print(f"Testing server: {AZURE_DELTASHARE_URL}")
    print(f"Bearer token: {'*' * len(bearer_token)}\n")
    
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        # Test 1: Health check
        print("1. Testing server health...")
        health_response = requests.get(f"{AZURE_DELTASHARE_URL}/health", timeout=10)
        if health_response.status_code == 200:
            print("   ✅ Server is healthy")
        else:
            print(f"   ⚠️  Health check returned {health_response.status_code}")
        
        # Test 2: List shares
        print("\n2. Listing shares...")
        shares_response = requests.get(f"{AZURE_DELTASHARE_URL}/shares", headers=headers, timeout=10)
        
        if shares_response.status_code == 200:
            shares_data = shares_response.json()
            shares = shares_data.get('items', [])
            print(f"   ✅ Found {len(shares)} shares:")
            for share in shares:
                print(f"      - {share.get('name', 'Unknown')}")
            
            if shares:
                # Test 3: List schemas in first share
                share_name = shares[0]['name']
                print(f"\n3. Listing schemas in '{share_name}'...")
                schemas_response = requests.get(
                    f"{AZURE_DELTASHARE_URL}/shares/{share_name}/schemas", 
                    headers=headers, 
                    timeout=10
                )
                
                if schemas_response.status_code == 200:
                    schemas_data = schemas_response.json()
                    schemas = schemas_data.get('items', [])
                    print(f"   ✅ Found {len(schemas)} schemas:")
                    for schema in schemas:
                        print(f"      - {schema.get('name', 'Unknown')}")
                    
                    if schemas:
                        # Test 4: List tables in first schema
                        schema_name = schemas[0]['name']
                        print(f"\n4. Listing tables in '{schema_name}'...")
                        tables_response = requests.get(
                            f"{AZURE_DELTASHARE_URL}/shares/{share_name}/schemas/{schema_name}/tables", 
                            headers=headers, 
                            timeout=10
                        )
                        
                        if tables_response.status_code == 200:
                            tables_data = tables_response.json()
                            tables = tables_data.get('items', [])
                            print(f"   ✅ Found {len(tables)} tables:")
                            for table in tables:
                                print(f"      - {table.get('name', 'Unknown')}")
                            
                            if tables:
                                # Test 5: Get metadata for first table
                                table_name = tables[0]['name']
                                print(f"\n5. Getting metadata for '{table_name}'...")
                                metadata_response = requests.get(
                                    f"{AZURE_DELTASHARE_URL}/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/metadata", 
                                    headers=headers, 
                                    timeout=10
                                )
                                
                                if metadata_response.status_code == 200:
                                    metadata = metadata_response.json()
                                    protocol = metadata.get('protocol', {})
                                    print(f"   ✅ Protocol version: {protocol.get('minReaderVersion', 'Unknown')}")
                                    
                                    # Test 6: Query table
                                    print(f"\n6. Querying '{table_name}' (limit 5)...")
                                    query_response = requests.post(
                                        f"{AZURE_DELTASHARE_URL}/shares/{share_name}/schemas/{schema_name}/tables/{table_name}/query", 
                                        headers=headers,
                                        json={"limitHint": 5},
                                        timeout=10
                                    )
                                    
                                    if query_response.status_code == 200:
                                        query_data = query_response.json()
                                        files = query_data.get('files', [])
                                        print(f"   ✅ Query returned {len(files)} file(s)")
                                        if files:
                                            print(f"   ✅ First file: {files[0].get('url', 'No URL')[:60]}...")
                                    else:
                                        print(f"   ❌ Query failed: {query_response.status_code} - {query_response.text}")
                                else:
                                    print(f"   ❌ Metadata request failed: {metadata_response.status_code}")
                        else:
                            print(f"   ❌ Tables request failed: {tables_response.status_code}")
                else:
                    print(f"   ❌ Schemas request failed: {schemas_response.status_code}")
        else:
            print(f"   ❌ Shares request failed: {shares_response.status_code} - {shares_response.text}")
            if shares_response.status_code == 401:
                print("   💡 Check your bearer token - authentication failed")
            return False
        
        print("\n🎉 Azure Delta Share deployment test completed successfully!")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: {e}")
        print("💡 Make sure the Azure Container Instance is running and accessible")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_azure_deltashare()
    sys.exit(0 if success else 1)