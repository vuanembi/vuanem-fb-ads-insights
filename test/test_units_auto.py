from .utils import process

ADS_ACCOUNT_ID = "act_3921338037921594"


def test_standard():
    data = {
        "ads_account_id": ADS_ACCOUNT_ID,
        "mode": "standard",
    }
    process(data)


# def test_hourly():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "hourly",
#     }
#     process(data)


# def test_age_genders():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "age_genders",
#     }
#     process(data)


# def test_devices():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "devices",
#     }
#     process(data)


def test_region():
    data = {
        "ads_account_id": ADS_ACCOUNT_ID,
        "mode": "country_region",
    }
    process(data)


# def test_ads_creatives():
#     data = {
#         "ads_account_id": ADS_ACCOUNT_ID,
#         "mode": "ads_creatives",
#     }
#     process(data)
