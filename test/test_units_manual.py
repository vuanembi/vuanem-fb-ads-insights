from .utils import process

ADS_ACCOUNT_ID = "act_3921338037921594"
START = "2021-07-01"
END = "2021-08-01"


def test_standard():
    data = {
        "ads_account_id": ADS_ACCOUNT_ID,
        "mode": "standard",
        "start": START,
        "end": END,
    }
    process(data)


# def test_hourly():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "hourly",
#         "start": START,
#         "end": END,
#     }
#     process(data)


# def test_age_genders():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "age_genders",
#         "start": START,
#         "end": END,
#     }
#     process(data)


# def test_devices():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "devices",
#         "start": START,
#         "end": END,
#     }
#     process(data)


def test_region():
    data = {
        "ads_account_id": ADS_ACCOUNT_ID,
        "mode": "country_region",
        "start": START,
        "end": END,
    }
    process(data)


# def test_ads_creatives():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "ads_creatives",
#         "start": START,
#         "end": END,
#     }
#     process(data)
