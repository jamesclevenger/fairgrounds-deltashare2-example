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

app = Flask(__name__)

# Bearer token for authentication
BEARER_TOKEN = os.getenv('DELTA_SHARING_BEARER_TOKEN', 'your-secure-bearer-token-here')

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
        return False
    token = auth_header.split(' ')[1]
    return token == BEARER_TOKEN

@app.before_request
def check_auth():
    """Check authentication for all requests except health"""
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
        return jsonify({"error": "Unauthorized"}), 401

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

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
    return jsonify({
        "items": [
            {
                "name": "fairgrounds_share",
                "id": "fairgrounds_share"
            }
        ]
    })

@app.route('/shares/<share_name>/schemas')
def list_schemas(share_name):
    """List schemas in a share"""
    if share_name != "fairgrounds_share":
        return jsonify({"error": "Share not found"}), 404
    
    return jsonify({
        "items": [
            {
                "name": "sample_data",
                "share": share_name
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
                "shareId": share_name
            },
            {
                "name": "orders", 
                "schema": "sample_data",
                "share": share_name,
                "shareId": share_name
            },
            {
                "name": "products",
                "schema": "sample_data",
                "share": share_name,
                "shareId": share_name
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
                "shareId": share_name
            },
            {
                "name": "orders", 
                "schema": schema_name,
                "share": share_name,
                "shareId": share_name
            },
            {
                "name": "products",
                "schema": schema_name,
                "share": share_name,
                "shareId": share_name
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
    
    # Mock metadata response
    return jsonify({
        "protocol": {
            "minReaderVersion": 1
        },
        "metaData": {
            "id": f"mock-{table_name}-id",
            "format": {
                "provider": "csv"
            },
            "schemaString": json.dumps({
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "integer", "nullable": True, "metadata": {}},
                    {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                    {"name": "data", "type": "string", "nullable": True, "metadata": {}}
                ]
            }),
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
    """Query table data"""
    if share_name != "fairgrounds_share" or schema_name != "sample_data":
        return jsonify({"error": "Table not found"}), 404
    
    if table_name not in ["customers", "orders", "products"]:
        return jsonify({"error": "Table not found"}), 404
    
    # Get the external URL for this container app
    external_url = request.host_url.rstrip('/')
    
    # Return proxy URL with token parameter instead of direct MinIO URL
    file_url = f"{external_url}/files/sample_data/{table_name}.csv?token={BEARER_TOKEN}"
    
    return jsonify({
        "protocol": {
            "minReaderVersion": 1
        },
        "files": [
            {
                "url": file_url,
                "id": f"mock-file-{table_name}",
                "partitionValues": {},
                "size": 1024,
                "stats": json.dumps({
                    "numRecords": 10,
                    "minValues": {},
                    "maxValues": {},
                    "nullCount": {}
                })
            }
        ]
    })

@app.route('/files/<path:object_path>')
def proxy_file(object_path):
    """Proxy file requests to MinIO"""
    try:
        print(f"Proxying file request for: {object_path}")
        print(f"MinIO endpoint: {MINIO_ENDPOINT}")
        print(f"MinIO bucket: {MINIO_BUCKET}")
        
        # Initialize MinIO if needed
        if not initialize_minio():
            print("Failed to initialize MinIO")
            return jsonify({"error": "Storage service unavailable"}), 503
        
        minio_client = get_minio_client()
        
        # Check if object exists
        try:
            stat = minio_client.stat_object(MINIO_BUCKET, object_path)
            print(f"Found object: {object_path}, size: {stat.size}")
        except S3Error as e:
            print(f"S3Error checking object: {e.code} - {e}")
            if e.code == 'NoSuchKey':
                return jsonify({"error": f"File not found: {object_path}"}), 404
            else:
                return jsonify({"error": f"Storage error: {e.code}"}), 500
        
        # Get object from MinIO
        try:
            print(f"Getting object from MinIO: {object_path}")
            response = minio_client.get_object(MINIO_BUCKET, object_path)
            
            # Read all data (simpler approach for small files)
            data = response.read()
            response.close()
            response.release_conn()
            
            print(f"Successfully retrieved {len(data)} bytes")
            
            # Return the file content
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

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    port = int(os.getenv('DELTA_SHARING_SERVER_PORT', 8080))
    host = os.getenv('DELTA_SHARING_SERVER_HOST', '0.0.0.0')
    
    print(f"Starting Mock Delta Sharing Server on {host}:{port}")
    print(f"Bearer Token: {BEARER_TOKEN}")
    
    app.run(host=host, port=port, debug=True)