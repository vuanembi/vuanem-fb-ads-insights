import requests

def test_auto():
    with requests.get('http://localhost:8080') as r:
        response = r.json()
    results = response.get('results')
    for i in results:
        assert i['start_date'] is not None
        assert i['end_date'] is not None
        assert i['num_processed'] > 0
