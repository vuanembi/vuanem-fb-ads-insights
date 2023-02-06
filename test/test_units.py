import pytest

from facebook.facebook_service import pipelines, pipeline_service
from facebook.facebook_controller import facebook_controller
from tasks.tasks_service import ACCOUNTS, tasks_service

TIMEFRAME = [
    # ("auto", (None, None)),
    ("manual", ("2023-02-01", "2023-03-01")),
]


@pytest.fixture(
    params=[i[1] for i in TIMEFRAME],
    ids=[i[0] for i in TIMEFRAME],
)
def timeframe(request):
    return request.param


@pytest.fixture(
    params=pipelines.values(),
    ids=pipelines.keys(),
)
def pipeline(request):
    return request.param


@pytest.fixture(params=ACCOUNTS)
def account(request):
    return request.param


class TestFacebook:
    def test_service(self, pipeline, account, timeframe):
        res = pipeline_service(pipeline)(account, timeframe[0], timeframe[1])
        res

    def test_controller(self, pipeline, account, timeframe):
        res = facebook_controller(
            {
                "table": pipeline.name,
                "ads_account_id": account,
                "start": timeframe[0],
                "end": timeframe[1],
            }
        )
        res


class TestTask:
    def test_service(self, timeframe):
        res = tasks_service(
            {
                "start": timeframe[0],
                "end": timeframe[1],
            }
        )
        res
