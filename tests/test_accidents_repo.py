import pytest
import json
from app import app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    client = app.test_client()
    yield client

def test_get_accidents_by_area(client):
    response = client.get('/accidents/area/1235')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, list)

def test_get_accidents_by_area_and_day(client):
    response = client.get('/accidents/area/1652/day/2023-02-06')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, list)

def test_get_accidents_by_area_and_start_date_for_week(client):
    response = client.get('/accidents/area/1652/week/2023-02-06')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, list)

def test_get_accidents_by_area_and_month(client):
    response = client.get('/accidents/area/1235/month/2023-09')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert isinstance(data, list)

def test_get_accidents_and_reason_by_area(client):
    response = client.get('/accidents/area/1652/reason')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'total_accidents' in data
    assert 'accidents' in data

def test_get_accidents_and_stats_injury_by_area(client):
    response = client.get('/accidents/area/1652/injuries')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'total_accidents' in data
    assert 'accidents' in data
