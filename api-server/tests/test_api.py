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
        {"symbol": "AAPL", "datetime": "2026-02-12T16:00:00", "price": 273.68}  # duplicate
    ]
    response = requests.post(f"{API_URL}/v1/load/features", headers=headers, data=json.dumps(payload))
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "partial"
    assert data["inserted_count"] == 0
    assert len(data["rejects"]) > 0
    assert "Duplicate key" in data["rejects"][0]["reason"]
