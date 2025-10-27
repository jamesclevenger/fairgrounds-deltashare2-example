#!/usr/bin/env python3
"""
Mock Delta Sharing server for testing purposes.
This implements the basic Delta Sharing REST API endpoints.
"""

from flask import Flask, request, jsonify, send_file
import os
import json
from datetime import datetime

app = Flask(__name__)

# Bearer token for authentication
BEARER_TOKEN = os.getenv('DELTA_SHARING_BEARER_TOKEN', 'your-secure-bearer-token-here')

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

@app.route('/files/<table_name>.csv')
def serve_csv_file(table_name):
    """Serve CSV files - simulates signed URL access"""
    if not verify_auth():
        return jsonify({"error": "Unauthorized"}), 401
    
    if table_name not in ["customers", "orders", "products"]:
        return jsonify({"error": "File not found"}), 404
    
    file_path = f"/data/{table_name}.csv"
    if os.path.exists(file_path):
        return send_file(file_path, mimetype='text/csv', as_attachment=False)
    else:
        return jsonify({"error": "File not found"}), 404

@app.route('/shares/<share_name>/schemas/<schema_name>/tables/<table_name>/query', methods=['POST'])
def query_table(share_name, schema_name, table_name):
    """Query table data"""
    if share_name != "fairgrounds_share" or schema_name != "sample_data":
        return jsonify({"error": "Table not found"}), 404
    
    if table_name not in ["customers", "orders", "products"]:
        return jsonify({"error": "Table not found"}), 404
    
    # Get server URL from request
    server_url = request.host_url.rstrip('/')
    
    # Return accessible HTTP URL instead of file:// path
    return jsonify({
        "protocol": {
            "minReaderVersion": 1
        },
        "files": [
            {
                "url": f"{server_url}/files/{table_name}.csv",
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