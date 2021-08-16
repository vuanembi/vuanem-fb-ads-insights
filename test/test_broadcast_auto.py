from .utils import process_broadcast


def test_standard():
    data = {"broadcast": "standard",}
    process_broadcast(data)

# def test_hourly():
#     data = {"broadcast": "hourly",}
#     process_broadcast(data)

# def test_age_genders():
#     data = {"broadcast": "age_genders",}
#     process_broadcast(data)

# def test_devices():
#     data = {"broadcast": "devices",}
#     process_broadcast(data)

def test_country_region():
    data = {"broadcast": "country_region",}
    process_broadcast(data)

# def test_ads_creatives():
#     data = {"broadcast": "ads_creatives",}
#     process_broadcast(data)
