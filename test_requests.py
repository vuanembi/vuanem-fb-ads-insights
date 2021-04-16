from unittest.mock import Mock

from main import main


def test_auto():
    data = {}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    for i in res["results"]:
        assert i["output_rows"] > 0
