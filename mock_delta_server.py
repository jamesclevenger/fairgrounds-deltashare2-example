#!/usr/bin/env python3
"""
Mock Delta Sharing server for testing purposes.
This implements the basic Delta Sharing REST API endpoints.
"""

from flask import Flask, request, jsonify
import os
import json
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import urllib3

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
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # HTTP for development
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
    
    if not verify_auth():
        return jsonify({"error": "Unauthorized"}), 401

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

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

def generate_presigned_url(object_name, expiry_hours=1):
    """Generate presigned URL for MinIO object"""
    try:
        minio_client = get_minio_client()
        url = minio_client.presigned_get_object(
            MINIO_BUCKET,
            object_name,
            expires=timedelta(hours=expiry_hours)
        )
        return url
    except S3Error as e:
        print(f"Error generating presigned URL: {e}")
        return None

@app.route('/shares/<share_name>/schemas/<schema_name>/tables/<table_name>/query', methods=['POST'])
def query_table(share_name, schema_name, table_name):
    """Query table data"""
    if share_name != "fairgrounds_share" or schema_name != "sample_data":
        return jsonify({"error": "Table not found"}), 404
    
    if table_name not in ["customers", "orders", "products"]:
        return jsonify({"error": "Table not found"}), 404
    
    # Generate presigned URL for MinIO object
    object_name = f"sample_data/{table_name}.csv"
    presigned_url = generate_presigned_url(object_name)
    
    if not presigned_url:
        return jsonify({"error": "Unable to generate file access URL"}), 500
    
    # Return MinIO presigned URL for direct data access
    return jsonify({
        "protocol": {
            "minReaderVersion": 1
        },
        "files": [
            {
                "url": presigned_url,
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