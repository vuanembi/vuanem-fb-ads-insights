from .utils import process_broadcast

START = "2021-01-01"
END = "2021-02-01"


def test_standard():
    data = {
        "broadcast": "standard",
        "start": START,
        "end": END,
    }
    process_broadcast(data)


# def test_hourly():
#     data = {
#         "broadcast": "hourly",
#         "start": START,
#         "end": END,
#     }
#     process_broadcast(data)


# def test_age_genders():
#     data = {
#         "broadcast": "age_genders",
#         "start": START,
#         "end": END,
#     }
#     process_broadcast(data)


# def test_devices():
#     data = {
#         "broadcast": "devices",
#         "start": START,
#         "end": END,
#     }
#     process_broadcast(data)


def test_country_region():
    data = {
        "broadcast": "country_region",
        "start": START,
        "end": END,
    }
    process_broadcast(data)


# def test_ads_creatives():
#     data = {
#         "broadcast": "ads_creatives",
#         "start": START,
#         "end": END,
#     }
#     process_broadcast(data)
