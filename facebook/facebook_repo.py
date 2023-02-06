from typing import Optional, Any
import json
from datetime import datetime
import time

import requests
from compose import compose

from secret_manager.cloud_secret_manager import get_secret

API_VER = "v15.0"
BASE_URL = f"https://graph.facebook.com/{API_VER}/"


def _request_async_report(session: requests.Session):
    def _request(
        level: str,
        fields: list[str],
        breakdowns: Optional[str],
        ads_account_id: str,
        start: datetime,
        end: datetime,
    ) -> str:
        params = {
            "level": level,
            "fields": json.dumps(fields),
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
                    "since": start.date().isoformat(),
                    "until": end.date().isoformat(),
                }
            ),
        }
        if breakdowns:
            params["breakdowns"] = breakdowns
        with session.post(
            f"{BASE_URL}/act_{ads_account_id}/insights",
            params=params,
        ) as r:
            r.raise_for_status()
            res = r.json()
        return res["report_run_id"]

    return _request


def _poll_async_report(session: requests.Session):
    def _poll(report_run_id: str) -> str:
        with session.get(
            f"{BASE_URL}/{report_run_id}",
        ) as r:
            res = r.json()
        if (
            res["async_percent_completion"] == 100
            and res["async_status"] == "Job Completed"
        ):
            return report_run_id
        elif res["async_status"] == "Job Failed":
            raise Exception(f"Async Failed {report_run_id}")
        else:
            time.sleep(5)
            return _poll(report_run_id)

    return _poll


def _get_insights(session: requests.Session):
    def _get(report_run_id):
        def __get(after: str = None) -> list[dict[str, Any]]:
            with session.get(
                f"{BASE_URL}/{report_run_id}/insights",
                params={
                    "limit": 500,
                    "after": after,
                },
            ) as r:
                res = r.json()
            data = res["data"]
            next_ = res["paging"].get("next")
            return data + __get(res["paging"]["cursors"]["after"]) if next_ else data

        return __get()

    return _get


def get(level: str, fields: list[str], breakdowns: Optional[str]):
    def _get(
        ads_account_id: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        with requests.Session() as session:
            session.params = {
                "access_token": get_secret(),
            }
            return compose(
                _get_insights(session),
                _poll_async_report(session),
                _request_async_report(session),
            )(
                level,
                fields,
                breakdowns,
                ads_account_id,
                start,
                end,
            )

    return _get
