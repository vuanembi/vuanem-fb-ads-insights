from aiohttp.http import RESPONSES
import requests

def test_auto():
    with requests.get('http://localhost:8080') as r:
        response = r.json()
    assert response['start_date'] is not None
    assert response['end_date'] is not None
    assert response['num_processed'] > 0
