import json
import base64
from datetime import datetime, timedelta
from unittest.mock import Mock

from main import main
from assertions import assertion

mock_context = Mock()
mock_context.event_id = "617187464135194"
mock_context.timestamp = "2019-07-15T22:09:03.761Z"


def test_manual():
    message = {
        "start_date": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
        "end_date": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
    }
    message_json = json.dumps(message)
    data = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(data, mock_context)
    assertion(res)

def test_auto():
    message = {}
    message_json = json.dumps(message)
    data = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(data, mock_context)
    assertion(res)

def test_manual_hourly():
    message = {
        "breakdowns": "hourly",
        "start_date": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
        "end_date": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
    }
    message_json = json.dumps(message)
    data = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(data, mock_context)
    assertion(res)

def test_auto_hourly():
    message = {
        "breakdowns": "hourly"
    }
    message_json = json.dumps(message)
    data = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(data, mock_context)
    assertion(res)
