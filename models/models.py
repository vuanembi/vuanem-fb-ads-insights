import os
import json
from datetime import datetime, timedelta
import time
import importlib
from abc import ABCMeta, abstractmethod

import requests
from google.cloud import bigquery

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

API_VER = "v12.0"
BASE_URL = f"https://graph.facebook.com/{API_VER}/"

BQ_CLIENT = bigquery.Client()
DATASET = "Facebook"

add_batched_at = lambda row: {
    **row,
    "_batched_at": NOW.isoformat(timespec="seconds"),
}


class FacebookAdsInsights(metaclass=ABCMeta):
    @staticmethod
    def factory(table, ads_account_id, start, end):
        try:
            module = importlib.import_module(f"models.{table}")
            model = getattr(module, table)
            return model(ads_account_id, start, end)
        except (ImportError, AttributeError):
            raise ValueError(table)

    windows = [
            "1d_click",
            "1d_view",
            "7d_click",
            "7d_view",
        ]

    @property
    @abstractmethod
    def keys(self):
        pass

    @property
    @abstractmethod
    def fields(self):
        pass

    @property
    @abstractmethod
    def level(self):
        pass
    @property
    @abstractmethod
    def breakdowns(self):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    def __init__(self, ads_account_id, start, end):
        self.table = self.__class__.__name__
        self.ads_account_id = ads_account_id
        self.start = (
            (NOW - timedelta(days=8))
            if not start
            else datetime.strptime(start, DATE_FORMAT)
        )
        self.end = NOW if not end else datetime.strptime(end, DATE_FORMAT)

    def _get_report_request(self, session, attempt=0):
        def _send_report_request():
            params = {
                "access_token": os.getenv("ACCESS_TOKEN"),
                "level": self.level,
                "fields": json.dumps(self.fields),
                "action_attribution_windows": json.dumps(self.windows),
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
                        "since": self.start.strftime(DATE_FORMAT),
                        "until": self.end.strftime(DATE_FORMAT),
                    }
                ),
            }
            if self.breakdowns:
                params["breakdowns"] = self.breakdowns
            with session.post(
                f"{BASE_URL}/{self.ads_account_id}/insights",
                params=params,
            ) as r:
                res = r.json()
            return res["report_run_id"]

        def _poll_report_request(report_run_id):
            with session.get(
                f"{BASE_URL}/{report_run_id}",
                params={
                    "access_token": os.getenv("ACCESS_TOKEN"),
                },
            ) as r:
                res = r.json()
            if (
                res["async_percent_completion"] == 100
                and res["async_status"] == "Job Completed"
            ):
                return report_run_id
            elif res["async_status"] == "Job Failed":
                return self._get_report_request(session, attempt + 1)
            else:
                time.sleep(10)
                return _poll_report_request(report_run_id)

        if attempt < 2:
            return _poll_report_request(_send_report_request())
        else:
            raise RuntimeError("Too many attempts")

    def _get_insights(self, session, report_run_id, after=None):
        params = {
            "access_token": os.getenv("ACCESS_TOKEN"),
            "limit": 500,
        }
        if after:
            params["after"] = after
        params
        with session.get(
            f"{BASE_URL}/{report_run_id}/insights",
            params=params,
        ) as r:
            res = r.json()
        data = res["data"]
        next_ = res["paging"].get("next")
        return (
            data
            + self._get_insights(
                session, report_run_id, res["paging"]["cursors"]["after"]
            )
            if next_
            else data
        )

    @abstractmethod
    def transform(self, rows):
        pass

    def _load(self, rows):
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{DATASET}.{self.table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=self.schema,
                ),
            )
            .result()
            .output_rows
        )
        self._update()
        return output_rows

    def _update(self):
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{self.table} AS
        SELECT * EXCEPT(row_num) FROM
        (
            SELECT *,
            ROW_NUMBER() OVER (
            PARTITION BY {','.join(self.keys['p_key'])}
            ORDER BY {self.keys['incre_key']} DESC) AS row_num
            FROM {DATASET}.{self.table}
        ) WHERE row_num = 1"""
        BQ_CLIENT.query(query).result()

    def run(self):
        with requests.Session() as session:
            rows = self._get_insights(session, self._get_report_request(session))
        response = {
            "table": self.table,
            "ads_account_id": self.ads_account_id,
            "start": self.start.strftime(DATE_FORMAT),
            "end": self.end.strftime(DATE_FORMAT),
            "num_processed": len(rows),
        }
        if rows:
            rows = self.transform(rows)
            rows = [add_batched_at(row) for row in rows]
            response["output_rows"] = self._load(rows)
        return response
