import requests
import json

API_URL = "http://localhost:8000"
API_KEY = "944f89421611f0e94c76b1234c540c7a01b4bf5de7521bdc3e1671b8577943ad"  # your key

headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

def test_health_endpoint():
    response = requests.get(f"{API_URL}/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"

def test_protected_endpoint_requires_key():
    response = requests.post(f"{API_URL}/v1/test-load")
    assert response.status_code == 401

def test_protected_endpoint_with_key():
    response = requests.post(f"{API_URL}/v1/test-load", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"

def test_load_features_success():
    payload = [
        {"symbol": "AAPL", "datetime": "2026-02-12T16:00:00", "price": 273.68},
        {"symbol": "MSFT", "datetime": "2026-02-12T16:00:00", "price": 300.50}
    ]
    response = requests.post(f"{API_URL}/v1/load/features", headers=headers, data=json.dumps(payload))
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["inserted_count"] >= 0

def test_load_features_duplicate_reject():
    payload = [
        {"symbol": "AAPL", "datetime": "duplicate-timestamp", "price": 273.68}  # Trigger mock rejection
    ]
    response = requests.post(f"{API_URL}/v1/load/features", headers=headers, data=json.dumps(payload))
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "partial"
    assert data["inserted_count"] == 0
    assert len(data["rejects"]) > 0
    assert "Duplicate key" in data["rejects"][0]["reason"]

# Tests for ML extraction endpoints

def test_get_schema():
    """Test the schema endpoint returns field information"""
    response = requests.get(f"{API_URL}/v1/extract/features/schema", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "fields" in data
    assert "recommended_ml_fields" in data
    assert len(data["fields"]) > 0
    # Check that fields have required properties
    assert "name" in data["fields"][0]
    assert "type" in data["fields"][0]
    assert "ml_relevant" in data["fields"][0]

def test_get_schema_requires_auth():
    """Test schema endpoint requires API key"""
    response = requests.get(f"{API_URL}/v1/extract/features/schema")
    assert response.status_code == 401

def test_get_stats():
    """Test the stats endpoint returns data statistics"""
    response = requests.get(f"{API_URL}/v1/extract/features/stats", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "total_records" in data
    assert "unique_symbols" in data
    assert "date_range" in data
    assert "symbols" in data
    assert isinstance(data["total_records"], int)
    assert isinstance(data["symbols"], list)

def test_get_stats_with_symbol_filter():
    """Test stats endpoint with symbol filtering"""
    response = requests.get(
        f"{API_URL}/v1/extract/features/stats?symbols=AAPL,MSFT", 
        headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert "symbols" in data
    # In test mode, should return mock data
    assert isinstance(data["symbols"], list)

def test_extract_features_json():
    """Test feature extraction in JSON format"""
    response = requests.get(
        f"{API_URL}/v1/extract/features?format=json&limit=10",
        headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "records" in data
    assert "count" in data
    assert "limit" in data
    assert "offset" in data
    assert data["limit"] == 10

def test_extract_features_csv():
    """Test feature extraction in CSV format"""
    response = requests.get(
        f"{API_URL}/v1/extract/features?format=csv&limit=5",
        headers=headers
    )
    assert response.status_code == 200
    assert "text/csv" in response.headers.get("content-type", "")
    # Check that response contains CSV data
    content = response.text
    assert len(content) > 0

def test_extract_features_with_filters():
    """Test extraction with various filters"""
    response = requests.get(
        f"{API_URL}/v1/extract/features?symbols=AAPL,MSFT&start_date=2026-01-01T00:00:00&end_date=2026-02-20T23:59:59&limit=100",
        headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["filters"]["symbols"] == "AAPL,MSFT"
    assert data["filters"]["start_date"] == "2026-01-01T00:00:00"
    assert data["filters"]["end_date"] == "2026-02-20T23:59:59"

def test_extract_features_with_field_selection():
    """Test extraction with specific field selection"""
    fields = "symbol,datetime,price,volume"
    response = requests.get(
        f"{API_URL}/v1/extract/features?fields={fields}&limit=5",
        headers=headers
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    # In test mode, mock records should be returned
    if data["count"] > 0:
        # Check that only requested fields are present
        record = data["records"][0]
        assert "symbol" in record
        assert "price" in record

def test_extract_features_invalid_fields():
    """Test that invalid field names are rejected"""
    response = requests.get(
        f"{API_URL}/v1/extract/features?fields=invalid_field,another_bad_field",
        headers=headers
    )
    assert response.status_code == 400
    data = response.json()
    assert "Invalid fields" in data["detail"]

def test_extract_features_pagination():
    """Test pagination with limit and offset"""
    # First page
    response1 = requests.get(
        f"{API_URL}/v1/extract/features?limit=5&offset=0",
        headers=headers
    )
    assert response1.status_code == 200
    data1 = response1.json()
    assert data1["limit"] == 5
    assert data1["offset"] == 0
    
    # Second page
    response2 = requests.get(
        f"{API_URL}/v1/extract/features?limit=5&offset=5",
        headers=headers
    )
    assert response2.status_code == 200
    data2 = response2.json()
    assert data2["limit"] == 5
    assert data2["offset"] == 5

def test_extract_features_requires_auth():
    """Test extraction endpoint requires API key"""
    response = requests.get(f"{API_URL}/v1/extract/features")
    assert response.status_code == 401

def test_extract_features_limit_validation():
    """Test that limit parameter is validated"""
    # Test upper limit
    response = requests.get(
        f"{API_URL}/v1/extract/features?limit=15000",
        headers=headers
    )
    assert response.status_code == 422  # Validation error
    
    # Test lower limit
    response = requests.get(
        f"{API_URL}/v1/extract/features?limit=0",
        headers=headers
    )
    assert response.status_code == 422  # Validation error
