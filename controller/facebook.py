import os
import json
from datetime import datetime
import time
from typing import Optional

import requests

from models.AdsInsights.base import FBAdsInsights

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

API_VER = "v12.0"
BASE_URL = f"https://graph.facebook.com/{API_VER}/"


class AsyncFailedException(Exception):
    def __init__(self, message):
        super().__init__(f"Async Job Failed: {message}")


ReportRunId = str
ReportRunRes = tuple[Optional[Exception], Optional[ReportRunId]]
Insight = dict
Insights = list[Insight]
InsightsRes = tuple[Optional[Exception], Optional[Insights]]


def request_async_report(
    session: requests.Session,
    model: FBAdsInsights,
    ads_account_id: str,
    start: datetime,
    end: datetime,
) -> ReportRunId:
    params = {
        "access_token": os.getenv("ACCESS_TOKEN"),
        "level": model["level"],
        "fields": json.dumps(model["fields"]),
        "action_attribution_windows": json.dumps(
            [
                "1d_click",
                "1d_view",
                "7d_click",
                "7d_view",
            ]
        ),
        "filtering": json.dumps(
            [
                {
                    "field": "ad.impressions",
                    "operator": "GREATER_THAN",
                    "value": 0,
                },
                {
                    "field": "ad.effective_status",
                    "operator": "IN",
                    "value": [
                        "ACTIVE",
                        "PAUSED",
                        "DELETED",
                        "PENDING_REVIEW",
                        "DISAPPROVED",
                        "PREAPPROVED",
                        "PENDING_BILLING_INFO",
                        "CAMPAIGN_PAUSED",
                        "ARCHIVED",
                        "ADSET_PAUSED",
                        "IN_PROCESS",
                        "WITH_ISSUES",
                    ],
                },
            ]
        ),
        "time_increment": 1,
        "time_range": json.dumps(
            {
                "since": start.strftime(DATE_FORMAT),
                "until": end.strftime(DATE_FORMAT),
            }
        ),
    }
    if model.get("breakdowns", ""):
        params["breakdowns"] = model["breakdowns"]
    with session.post(f"{BASE_URL}/{ads_account_id}/insights", params=params) as r:
        res = r.json()
    return res["report_run_id"]


def poll_async_report(
    session: requests.Session, report_run_id: ReportRunId
) -> ReportRunId:
    with session.get(
        f"{BASE_URL}/{report_run_id}",
        params={"access_token": os.getenv("ACCESS_TOKEN")},
    ) as r:
        res = r.json()
    if (
        res["async_percent_completion"] == 100
        and res["async_status"] == "Job Completed"
    ):
        return report_run_id
    elif res["async_status"] == "Job Failed":
        raise AsyncFailedException(report_run_id)
    else:
        print(res["async_percent_completion"])
        time.sleep(5)
        return poll_async_report(session, report_run_id)


def get_async_report(
    session: requests.Session,
    model: FBAdsInsights,
    ads_account_id: str,
    start: datetime,
    end: datetime,
    attempt: int = 0,
) -> ReportRunId:
    report_run_id = request_async_report(
        session,
        model,
        ads_account_id,
        start,
        end,
    )
    try:
        return poll_async_report(session, report_run_id)
    except AsyncFailedException as e:
        if attempt < 5:
            return get_async_report(
                session,
                model,
                ads_account_id,
                start,
                end,
                attempt + 1,
            )
        else:
            raise e


def get_insights(
    session: requests.Session,
    report_run_id: ReportRunId,
    after: str = None,
) -> InsightsRes:
    try:
        with session.get(
            f"{BASE_URL}/{report_run_id}/insights",
            params={
                "access_token": os.getenv("ACCESS_TOKEN"),
                "limit": 500,
                "after": after,
            },
        ) as r:
            res = r.json()
        data = res["data"]
        next_ = res["paging"].get("next")
        return (
            data
            + get_insights(session, report_run_id, res["paging"]["cursors"]["after"])
            if next_
            else data
        )
    except KeyError:
        return get_insights(session, report_run_id, after)


def get(
    session: requests.Session,
    model: FBAdsInsights,
    ads_account_id: str,
    start: datetime,
    end: datetime,
) -> InsightsRes:
    return get_insights(
        session,
        get_async_report(
            session,
            model,
            ads_account_id,
            start,
            end,
        ),
    )
