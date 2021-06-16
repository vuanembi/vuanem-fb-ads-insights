from datetime import datetime, timedelta
from unittest.mock import Mock

from main import main
from .utils import assertion, encode_data


def test_auto():
    data = {"ads_account_id": "act_3921338037921594"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_manual_standard():
    data = {
        "ads_account_id": "act_2419414334994459",
        "start": "2021-05-01",
        "end": "2021-06-01",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_manual_hourly():
    data = {
        "ads_account_id": "act_2183630621872032",
        "mode": "hourly",
        "start": (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        "end": (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_manual_age_genders():
    data = {
        "ads_account_id": "act_2183630621872032",
        "mode": "age_genders",
        "start": (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        "end": (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_manual_devices():
    data = {
        "ads_account_id": "act_2183630621872032",
        "mode": "devices",
        "start": (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        "end": (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_manual_region():
    data = {
        "ads_account_id": "act_2183630621872032",
        "mode": "country_region",
        "start": (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d"),
        "end": (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_manual_ads_creatives():
    data = {
        "ads_account_id": "act_28834500",
        "mode": "ads_creatives",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_broadcast_ads_insights():
    data = {"broadcast": "ads_insights"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assert res["message_sent"] > 0


def test_broadcast_ads_creatives():
    data = {"broadcast": "ads_creatives"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assert res["message_sent"] > 0
