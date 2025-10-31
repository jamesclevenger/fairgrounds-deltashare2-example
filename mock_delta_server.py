#!/usr/bin/env python3
"""
Mock Delta Sharing server for testing purposes.
This implements the basic Delta Sharing REST API endpoints.
"""

from flask import Flask, request, jsonify, Response
import os
import json
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import urllib3
import requests
import uuid

app = Flask(__name__)

# Bearer token for authentication
BEARER_TOKEN = os.getenv('DELTA_SHARING_BEARER_TOKEN', 'your-secure-bearer-token-here')

# Fixed UUIDs for consistent responses
SHARE_ID = "550e8400-e29b-41d4-a716-446655440000"
SCHEMA_ID = "550e8400-e29b-41d4-a716-446655440001"
TABLE_IDS = {
    "customers": "550e8400-e29b-41d4-a716-446655440002",
    "orders": "550e8400-e29b-41d4-a716-446655440003", 
    "products": "550e8400-e29b-41d4-a716-446655440004"
}

# MinIO configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'fairgrounds-deltashare-development-minio.eastus.azurecontainer.io:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin123')
MINIO_BUCKET = os.getenv('MINIO_BUCKET_NAME', 'delta-sharing-data')

# Initialize MinIO client
def get_minio_client():
    # Disable SSL warnings for HTTP connections
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,  # HTTP for development
        http_client=urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=5, read=10),
            retries=urllib3.Retry(total=3, backoff_factor=0.3)
        )
    )

def verify_auth():
    """Verify bearer token authentication"""
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        print(f"Missing or invalid auth header: {auth_header}")
        return False
    
    try:
        token = auth_header.split(' ')[1]
        is_valid = token == BEARER_TOKEN
        if not is_valid:
            print(f"Invalid token provided: {token[:10]}... (expected: {BEARER_TOKEN[:10]}...)")
        return is_valid
    except IndexError:
        print("Malformed Authorization header")
        return False

@app.before_request
def check_auth():
    """Check authentication for all requests except health"""
    # Log all requests for debugging
    print(f"Request: {request.method} {request.path}")
    print(f"Query params: {dict(request.args)}")
    print(f"Endpoint: {request.endpoint}")
    print(f"User-Agent: {request.headers.get('User-Agent', 'N/A')}")
    if request.method == 'POST' and request.is_json:
        print(f"Request body: {request.get_json()}")
    
    if request.endpoint == 'health':
        return
    
    # For file proxy, check token in query parameter or header
    if request.endpoint == 'proxy_file':
        # Check for token in query parameter first
        token = request.args.get('token')
        if token and token == BEARER_TOKEN:
            return
        # Fall through to header check
    
    if not verify_auth():
        print("Authentication failed")
        return jsonify({"error": "Unauthorized"}), 401

@app.after_request
def after_request(response):
    """Log responses for debugging"""
    print(f"Response: {response.status_code}")
    if response.content_type and 'application/json' in response.content_type:
        try:
            print(f"Response body: {response.get_data(as_text=True)}")
        except:
            print("Could not log response body")
    return response

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

@app.route('/debug/responses')
def debug_responses():
    """Test all endpoint responses to ensure they're properly formatted"""
    endpoints = {}
    
    # Test shares endpoint
    try:
        from unittest.mock import Mock
        with app.test_request_context('/shares', headers={'Authorization': f'Bearer {BEARER_TOKEN}'}):
            response = list_shares()
            endpoints['/shares'] = {
                "status": "success",
                "data": response.get_json() if hasattr(response, 'get_json') else str(response)
            }
    except Exception as e:
        endpoints['/shares'] = {"status": "error", "error": str(e)}
    
    # Test specific share endpoint
    try:
        with app.test_request_context('/shares/fairgrounds_share', headers={'Authorization': f'Bearer {BEARER_TOKEN}'}):
            response = get_share('fairgrounds_share')
            endpoints['/shares/fairgrounds_share'] = {
                "status": "success", 
                "data": response.get_json() if hasattr(response, 'get_json') else str(response)
            }
    except Exception as e:
        endpoints['/shares/fairgrounds_share'] = {"status": "error", "error": str(e)}
    
    return jsonify({
        "test_results": endpoints,
        "bearer_token": BEARER_TOKEN[:10] + "...",
        "expected_share_name": "fairgrounds_share"
    })

@app.route('/debug/minio')
def debug_minio():
    """Debug MinIO connection and bucket contents"""
    try:
        minio_client = get_minio_client()
        
        # Check bucket exists
        bucket_exists = minio_client.bucket_exists(MINIO_BUCKET)
        
        # List objects if bucket exists
        objects = []
        if bucket_exists:
            try:
                for obj in minio_client.list_objects(MINIO_BUCKET, recursive=True):
                    objects.append({
                        "name": obj.object_name,
                        "size": obj.size,
                        "last_modified": obj.last_modified.isoformat() if obj.last_modified else None
                    })
            except Exception as e:
                objects = [f"Error listing objects: {e}"]
        
        return jsonify({
            "minio_endpoint": MINIO_ENDPOINT,
            "bucket_name": MINIO_BUCKET,
            "bucket_exists": bucket_exists,
            "objects": objects[:10]  # Limit to first 10 objects
        })
        
    except Exception as e:
        return jsonify({
            "error": f"MinIO connection failed: {type(e).__name__}: {e}",
            "minio_endpoint": MINIO_ENDPOINT,
            "bucket_name": MINIO_BUCKET
        }), 500

@app.route('/shares')
def list_shares():
    """List all shares"""
    # Support pagination parameters (even if not used)
    max_results = request.args.get('maxResults', type=int)
    page_token = request.args.get('pageToken')
    
    response = {
        "items": [
            {
                "name": "fairgrounds_share",
                "id": SHARE_ID
            }
        ]
    }
    
    # Add nextPageToken if pagination is needed (not needed for single share)
    # if has_more_results:
    #     response["nextPageToken"] = "next_token_here"
    
    return jsonify(response)

@app.route('/shares/<share_name>')
def get_share(share_name):
    """Get specific share information"""
    print(f"Getting share info for: '{share_name}'")
    
    if share_name != "fairgrounds_share":
        print(f"Share not found: '{share_name}' != 'fairgrounds_share'")
        return jsonify({"error": "Share not found"}), 404
    
    # According to Delta Sharing protocol, response should wrap share in "share" field
    response_data = {
        "share": {
            "name": "fairgrounds_share",
            "id": SHARE_ID
        }
    }
    print(f"Returning share data: {response_data}")
    return jsonify(response_data)

@app.route('/shares/<share_name>/schemas')
def list_schemas(share_name):
    """List schemas in a share"""
    if share_name != "fairgrounds_share":
        return jsonify({"error": "Share not found"}), 404
    
    # Support pagination parameters
    max_results = request.args.get('maxResults', type=int)
    page_token = request.args.get('pageToken')
    
    return jsonify({
        "items": [
            {
                "name": "sample_data",
                "share": share_name,
                "id": SCHEMA_ID
            }
        ]
    })

@app.route('/shares/<share_name>/all-tables')
def list_all_tables(share_name):
    """List all tables in a share (Databricks specific endpoint)"""
    if share_name != "fairgrounds_share":
        return jsonify({"error": "Share not found"}), 404
    
    return jsonify({
        "items": [
            {
                "name": "customers",
                "schema": "sample_data",
                "share": share_name,
                "shareId": SHARE_ID,
                "id": TABLE_IDS["customers"]
            },
            {
                "name": "orders", 
                "schema": "sample_data",
                "share": share_name,
                "shareId": SHARE_ID,
                "id": TABLE_IDS["orders"]
            },
            {
                "name": "products",
                "schema": "sample_data",
                "share": share_name,
                "shareId": SHARE_ID,
                "id": TABLE_IDS["products"]
            }
        ]
    })

@app.route('/shares/<share_name>/schemas/<schema_name>/tables')
def list_tables(share_name, schema_name):
    """List tables in a schema"""
    if share_name != "fairgrounds_share" or schema_name != "sample_data":
        return jsonify({"error": "Schema not found"}), 404
    
    return jsonify({
        "items": [
            {
                "name": "customers",
                "schema": schema_name,
                "share": share_name,
                "shareId": SHARE_ID,
                "id": TABLE_IDS["customers"]
            },
            {
                "name": "orders", 
                "schema": schema_name,
                "share": share_name,
                "shareId": SHARE_ID,
                "id": TABLE_IDS["orders"]
            },
            {
                "name": "products",
                "schema": schema_name,
                "share": share_name,
                "shareId": SHARE_ID,
                "id": TABLE_IDS["products"]
            }
        ]
    })

@app.route('/shares/<share_name>/schemas/<schema_name>/tables/<table_name>/metadata')
def get_table_metadata(share_name, schema_name, table_name):
    """Get table metadata"""
    if share_name != "fairgrounds_share" or schema_name != "sample_data":
        return jsonify({"error": "Table not found"}), 404
    
    if table_name not in ["customers", "orders", "products"]:
        return jsonify({"error": "Table not found"}), 404
    
    # Enhanced metadata response with more realistic schema
    table_schemas = {
        "customers": {
            "type": "struct",
            "fields": [
                {"name": "customer_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "customer_name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "email", "type": "string", "nullable": True, "metadata": {}},
                {"name": "created_at", "type": "string", "nullable": True, "metadata": {}}
            ]
        },
        "orders": {
            "type": "struct", 
            "fields": [
                {"name": "order_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "customer_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "order_date", "type": "string", "nullable": True, "metadata": {}},
                {"name": "total_amount", "type": "decimal(10,2)", "nullable": True, "metadata": {}}
            ]
        },
        "products": {
            "type": "struct",
            "fields": [
                {"name": "product_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "product_name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "price", "type": "decimal(10,2)", "nullable": True, "metadata": {}},
                {"name": "category", "type": "string", "nullable": True, "metadata": {}}
            ]
        }
    }
    
    schema = table_schemas.get(table_name, table_schemas["customers"])
    
    return jsonify({
        "protocol": {
            "minReaderVersion": 1
        },
        "metaData": {
            "id": TABLE_IDS.get(table_name, str(uuid.uuid4())),
            "name": table_name,
            "format": {
                "provider": "parquet"
            },
            "schemaString": json.dumps(schema),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": int(datetime.now().timestamp() * 1000)
        }
    })

def initialize_minio():
    """Initialize MinIO bucket and upload sample data"""
    try:
        print(f"Initializing MinIO at {MINIO_ENDPOINT}")
        minio_client = get_minio_client()
        
        # Create bucket if it doesn't exist
        if not minio_client.bucket_exists(MINIO_BUCKET):
            print(f"Creating bucket {MINIO_BUCKET}")
            minio_client.make_bucket(MINIO_BUCKET)
        else:
            print(f"Bucket {MINIO_BUCKET} already exists")
        
        # Upload sample data files
        sample_files = ['customers.csv', 'orders.csv', 'products.csv']
        for filename in sample_files:
            local_path = f'/data/{filename}'
            object_name = f'sample_data/{filename}'
            
            if os.path.exists(local_path):
                try:
                    # Check if object already exists
                    minio_client.stat_object(MINIO_BUCKET, object_name)
                    print(f"Object {object_name} already exists")
                except S3Error as e:
                    if e.code == 'NoSuchKey':
                        # Upload the file
                        print(f"Uploading {local_path} to {object_name}")
                        minio_client.fput_object(MINIO_BUCKET, object_name, local_path)
                        print(f"Successfully uploaded {object_name}")
                    else:
                        raise
            else:
                print(f"Warning: Local file {local_path} not found")
        
        print("MinIO initialization completed successfully")
        return True
        
    except Exception as e:
        print(f"Error initializing MinIO: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

def generate_presigned_url(object_name, expiry_hours=1):
    """Generate presigned URL for MinIO object"""
    try:
        print(f"Connecting to MinIO at {MINIO_ENDPOINT} for object {object_name}")
        minio_client = get_minio_client()
        
        # Check if bucket exists
        if not minio_client.bucket_exists(MINIO_BUCKET):
            print(f"Bucket {MINIO_BUCKET} does not exist, attempting to initialize...")
            if not initialize_minio():
                return None
        
        # Check if object exists
        try:
            minio_client.stat_object(MINIO_BUCKET, object_name)
            print(f"Object {object_name} found in bucket {MINIO_BUCKET}")
        except S3Error as e:
            if e.code == 'NoSuchKey':
                print(f"Object {object_name} not found, attempting to initialize...")
                if not initialize_minio():
                    return None
                # Try again after initialization
                try:
                    minio_client.stat_object(MINIO_BUCKET, object_name)
                except S3Error:
                    print(f"Error: Object {object_name} still not found after initialization")
                    return None
            else:
                raise
        
        # Generate presigned URL
        url = minio_client.presigned_get_object(
            MINIO_BUCKET,
            object_name,
            expires=timedelta(hours=expiry_hours)
        )
        print(f"Generated presigned URL: {url[:100]}...")
        return url
        
    except Exception as e:
        print(f"Error generating presigned URL: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None

@app.route('/shares/<share_name>/schemas/<schema_name>/tables/<table_name>/query', methods=['POST'])
def query_table(share_name, schema_name, table_name):
    """Query table data - returns NDJSON format as per Delta Sharing protocol"""
    print(f"=== QUERY REQUEST for {table_name} ===")
    print(f"Request body: {request.get_data()}")
    print(f"Headers: {dict(request.headers)}")
    
    if share_name != "fairgrounds_share" or schema_name != "sample_data":
        return jsonify({"error": "Table not found"}), 404
    
    if table_name not in ["customers", "orders", "products"]:
        return jsonify({"error": "Table not found"}), 404
    
    # Get the external URL for this container app - ensure HTTPS
    external_url = request.host_url.rstrip('/')
    if external_url.startswith('http://'):
        external_url = external_url.replace('http://', 'https://')
    
    # Return proxy URL with token parameter instead of direct MinIO URL
    file_url = f"{external_url}/files/sample_data/{table_name}.parquet?token={BEARER_TOKEN}"
    
    # Get table schema for metadata
    table_schemas = {
        "customers": {
            "type": "struct",
            "fields": [
                {"name": "customer_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "customer_name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "email", "type": "string", "nullable": True, "metadata": {}},
                {"name": "created_at", "type": "string", "nullable": True, "metadata": {}}
            ]
        },
        "orders": {
            "type": "struct", 
            "fields": [
                {"name": "order_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "customer_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "order_date", "type": "string", "nullable": True, "metadata": {}},
                {"name": "total_amount", "type": "decimal(10,2)", "nullable": True, "metadata": {}}
            ]
        },
        "products": {
            "type": "struct",
            "fields": [
                {"name": "product_id", "type": "integer", "nullable": False, "metadata": {}},
                {"name": "product_name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "price", "type": "decimal(10,2)", "nullable": True, "metadata": {}},
                {"name": "category", "type": "string", "nullable": True, "metadata": {}}
            ]
        }
    }
    
    schema = table_schemas.get(table_name, table_schemas["customers"])
    
    # Build NDJSON response as per Delta Sharing protocol
    # Line 1: Protocol object
    protocol_line = json.dumps({
        "protocol": {
            "minReaderVersion": 1
        }
    })
    
    # Line 2: Metadata object  
    metadata_line = json.dumps({
        "metaData": {
            "id": TABLE_IDS.get(table_name, str(uuid.uuid4())),
            "name": table_name,
            "format": {
                "provider": "parquet"
            },
            "schemaString": json.dumps(schema),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": int(datetime.now().timestamp() * 1000)
        }
    })
    
    # Line 3: File object
    file_line = json.dumps({
        "file": {
            "url": file_url,
            "id": str(uuid.uuid4()),
            "partitionValues": {},
            "size": 1024,
            "timestamp": int(datetime.now().timestamp() * 1000),
            "stats": json.dumps({
                "numRecords": 10,
                "minValues": {},
                "maxValues": {},
                "nullCount": {}
            })
        }
    })
    
    # Combine lines with newlines for NDJSON format (3 lines only)
    ndjson_response = f"{protocol_line}\n{metadata_line}\n{file_line}\n"
    
    print(f"=== RETURNING NDJSON RESPONSE ===")
    print(f"Response body: {ndjson_response}")
    print(f"Headers: Delta-Table-Version: 1")
    
    # Return with proper headers including Delta-Table-Version
    return Response(
        ndjson_response,
        mimetype='application/x-ndjson; charset=utf-8',
        headers={
            'Content-Type': 'application/x-ndjson; charset=utf-8',
            'Delta-Table-Version': '1'
        }
    )

@app.route('/files/<path:object_path>')
def proxy_file(object_path):
    """Proxy file requests to MinIO or return mock Parquet data"""
    try:
        print(f"Proxying file request for: {object_path}")
        
        # If requesting .parquet file, create mock Parquet data
        if object_path.endswith('.parquet'):
            return create_mock_parquet_response(object_path)
        
        # For CSV files, continue with MinIO proxy
        print(f"MinIO endpoint: {MINIO_ENDPOINT}")
        print(f"MinIO bucket: {MINIO_BUCKET}")
        
        # Initialize MinIO if needed
        if not initialize_minio():
            print("Failed to initialize MinIO")
            return jsonify({"error": "Storage service unavailable"}), 503
        
        minio_client = get_minio_client()
        
        # Convert .parquet request to .csv for MinIO
        csv_path = object_path.replace('.parquet', '.csv')
        
        # Check if object exists
        try:
            stat = minio_client.stat_object(MINIO_BUCKET, csv_path)
            print(f"Found object: {csv_path}, size: {stat.size}")
        except S3Error as e:
            print(f"S3Error checking object: {e.code} - {e}")
            if e.code == 'NoSuchKey':
                return jsonify({"error": f"File not found: {csv_path}"}), 404
            else:
                return jsonify({"error": f"Storage error: {e.code}"}), 500
        
        # Get object from MinIO
        try:
            print(f"Getting object from MinIO: {csv_path}")
            response = minio_client.get_object(MINIO_BUCKET, csv_path)
            
            # Read all data (simpler approach for small files)
            data = response.read()
            response.close()
            response.release_conn()
            
            print(f"Successfully retrieved {len(data)} bytes")
            
            # Return the file content as CSV (for now)
            return Response(data, mimetype='text/csv', headers={
                'Content-Disposition': f'attachment; filename="{object_path.split("/")[-1]}"'
            })
            
        except Exception as e:
            print(f"Error reading from MinIO: {type(e).__name__}: {e}")
            return jsonify({"error": f"Failed to read file: {str(e)}"}), 500
        
    except Exception as e:
        print(f"Error proxying file: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Internal error: {str(e)}"}), 500

def create_mock_parquet_response(object_path):
    """Create a mock Parquet file response"""
    try:
        # For now, just return the CSV data with Parquet mimetype
        # In a real implementation, you'd convert CSV to Parquet format
        table_name = object_path.split('/')[-1].replace('.parquet', '')
        
        # Simple CSV data that mimics what would be in Parquet
        if table_name == 'customers':
            csv_data = """customer_id,customer_name,email,created_at
1,John Doe,john@example.com,2024-01-01 10:00:00
2,Jane Smith,jane@example.com,2024-01-02 11:00:00
3,Bob Johnson,bob@example.com,2024-01-03 12:00:00"""
        elif table_name == 'orders':
            csv_data = """order_id,customer_id,order_date,total_amount
101,1,2024-01-01 10:30:00,99.99
102,2,2024-01-02 11:30:00,149.99
103,1,2024-01-03 12:30:00,79.99"""
        elif table_name == 'products':
            csv_data = """product_id,product_name,price,category
1,Widget A,29.99,Electronics
2,Widget B,39.99,Electronics
3,Gadget C,19.99,Accessories"""
        else:
            csv_data = "id,name,value\n1,Sample,123"
        
        print(f"Returning mock Parquet data for {table_name}")
        
        # Return as Parquet mimetype (even though it's CSV data)
        # In a real implementation, you'd use a library like pyarrow to create actual Parquet
        return Response(
            csv_data.encode('utf-8'), 
            mimetype='application/octet-stream',
            headers={
                'Content-Disposition': f'attachment; filename="{object_path.split("/")[-1]}"',
                'Content-Type': 'application/octet-stream'
            }
        )
        
    except Exception as e:
        print(f"Error creating mock Parquet: {e}")
        return jsonify({"error": f"Failed to create mock data: {str(e)}"}), 500

@app.errorhandler(404)
def not_found(error):
    """Enhanced 404 handler with request logging"""
    print(f"404 Not Found: {request.method} {request.path}")
    print(f"Query params: {dict(request.args)}")
    print(f"Headers: {dict(request.headers)}")
    return jsonify({"error": "Not found"}), 404

@app.route('/<path:path>')
def catch_all(path):
    """Catch-all route for debugging missing endpoints"""
    print(f"Unhandled request: {request.method} /{path}")
    print(f"Query params: {dict(request.args)}")
    print(f"Headers: {dict(request.headers)}")
    return jsonify({
        "error": "Endpoint not implemented",
        "method": request.method,
        "path": f"/{path}",
        "available_endpoints": [
            "GET /shares",
            "GET /shares/{share}",
            "GET /shares/{share}/schemas",
            "GET /shares/{share}/all-tables",
            "GET /shares/{share}/schemas/{schema}/tables",
            "GET /shares/{share}/schemas/{schema}/tables/{table}/metadata",
            "POST /shares/{share}/schemas/{schema}/tables/{table}/query",
            "GET /files/{path}",
            "GET /health",
            "GET /debug/minio"
        ]
    }), 404

@app.errorhandler(400)
def bad_request(error):
    """Handle 400 Bad Request errors"""
    print(f"400 Bad Request: {error}")
    return jsonify({"error": "Bad request"}), 400

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 Internal Server Error"""
    print(f"500 Internal Server Error: {error}")
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    port = int(os.getenv('DELTA_SHARING_SERVER_PORT', 8080))
    host = os.getenv('DELTA_SHARING_SERVER_HOST', '0.0.0.0')
    
    print(f"Starting Mock Delta Sharing Server on {host}:{port}")
    print(f"Bearer Token: {BEARER_TOKEN}")
    
    app.run(host=host, port=port, debug=True)