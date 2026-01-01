import functions_framework
from google.cloud import bigquery
from datetime import datetime
import json

# Initialize BigQuery client
client = bigquery.Client()
dataset_id = 'jhadoo_analytics'
table_id = 'cleanup_stats'

@functions_framework.http
def receive_telemetry(request):
    """HTTP Cloud Function to receive Jhadoo telemetry."""
    
    # Handle CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    if request.method != 'POST':
        return ('Only POST supported', 405, headers)

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return ('Invalid JSON', 400, headers)
            
        # Validate required fields
        required = ['user_id', 'bytes_saved', 'timestamp', 'os']
        if not all(k in request_json for k in required):
            return ('Missing required fields', 400, headers)
            
        # Prepare row for BigQuery
        row = {
            "user_id": str(request_json['user_id']),
            "bytes_saved": int(request_json['bytes_saved']),
            "duration_seconds": float(request_json.get('duration_seconds', 0.0)),
            "timestamp": request_json['timestamp'], # Standard ISO format
            "os": str(request_json['os']),
            "version": str(request_json.get('version', 'unknown')),
            "python_version": str(request_json.get('python_version', 'unknown')),
            "server_timestamp": datetime.utcnow().isoformat()
        }
        
        # Insert into BigQuery
        table_ref = client.dataset(dataset_id).table(table_id)
        errors = client.insert_rows_json(table_ref, [row])
        
        if errors:
            print(f"BigQuery Insert Errors: {errors}")
            return ('Error inserting data', 500, headers)
            
        return ('OK', 200, headers)
        
    except Exception as e:
        print(f"Error: {e}")
        return (f'Internal Error: {str(e)}', 500, headers)
