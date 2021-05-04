import os
import json
import time
import base64
from datetime import datetime, timedelta
from urllib3.util.retry import Retry


import requests
from requests.adapters import HTTPAdapter
from google.cloud import bigquery
import jinja2


class FacebookAdsInsights:
    def __init__(self, **kwargs):
        """Init. Getting env & configs from envs"""

        self.api_ver = os.getenv("API_VER")
        self.ads_account = os.getenv("ADS_ACCOUNT")
        self.access_token = os.getenv("ACCESS_TOKEN")

        self.dataset = "Facebook"
        self.table = os.getenv("TABLE")
        self.base_url = f"https://graph.facebook.com/{self.api_ver}/"

        if "start_date" in kwargs and "end_date" in kwargs:
            self.manual = True
            self.start_date = kwargs["start_date"]
            self.end_date = kwargs["end_date"]
        else:
            self.manual = False
            self.end_date = datetime.now().strftime("%Y-%m-%d")
            self.start_date = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")

        self.num_processed = 0
        self.job_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        config = self._init_config()
        self.field = config.get("field")
        self.fields = self.field.get("fields")
        self.windows = self.field.get("action_attribution_windows")
        self.fields_with_windows = self.field.get("fields_with_windows")
        self.key = config.get("key")

        self.session = self._init_session()

        self.client = bigquery.Client()

    def _init_config(self):
        with open(f"configs/config.json", "r") as f:
            config = json.load(f)
        return config

    def _init_session(self):
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[100, 413, 429, 503],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _init_table(self, table):
        return f"Ads_Insights_{table}"

    def fetch(self):
        report_run_id = self._fetch_report_id()
        report_run_id = self._poll_report_id(report_run_id)
        return self._fetch_insights(report_run_id)

    def _breakdowns_params_factory(self, params):
        return params

    def _fetch_report_id(self):
        url = self.base_url + f"{self.ads_account}/insights"
        time_range = {"since": self.start_date, "until": self.end_date}
        params = {
            "access_token": self.access_token,
            "level": "ad",
            "time_range": json.dumps(time_range),
            "time_increment": 1,
            "fields": json.dumps(self.fields),
            "action_attribution_windows": json.dumps(self.windows),
        }
        params = self._breakdowns_params_factory(params)
        with self.session.post(url, params=params) as r:
            res = r.json()
        return res["report_run_id"]

    def _poll_report_id(self, report_run_id):
        url = self.base_url + report_run_id
        while True:
            params = {"access_token": self.access_token}
            with self.session.get(url, params=params) as r:
                res = r.json()
            if (res["async_status"] == "Job Completed") and (
                res["async_percent_completion"] == 100
            ):
                return report_run_id
            elif res["async_status"] in ["Job Failed", "Job Skipped"]:
                raise RuntimeError
            else:
                print(res["async_percent_completion"])
                time.sleep(3)

    def _fetch_insights(self, report_run_id):
        url = self.base_url + f"{report_run_id}/insights"
        rows = []
        after = None
        while True:
            params = {"access_token": self.access_token, "limit": 500}
            if after is not None:
                params["after"] = after
            with self.session.get(url, params=params) as r:
                res = r.json()
            data = res.get("data")
            rows.extend(data)
            next = res.get("paging").get("next")
            if next is not None:
                after = res.get("paging").get("cursors").get("after")
            else:
                break
        return rows

    def transform(self, rows):
        rows = [self._transform_prefix(row) for row in rows]
        rows = [dict(row, **{"_batched_at": self.job_ts}) for row in rows]
        self.num_processed = len(rows)
        return rows

    def _transform_prefix(self, row):
        """Transform fields name to adhere to columns' naming requirements

        Args:
            result (dict): Individual result

        Returns:
            dict: Processed individual result
        """

        for nest in self.fields_with_windows:
            fields = row.get(nest)
            if fields:
                for i in fields:
                    for k in [j for j in i.keys() if j not in ["value", "action_type"]]:
                        i["_" + k] = i.pop(k)
        return row

    def load(self, rows):
        schema = self._schema_factory()

        return self.client.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

    def _schema_factory(self):
        with open("schemas/AdsInsights.json") as f:
            schema = json.load(f)
        return schema

    def update(self):
        loader = jinja2.FileSystemLoader(searchpath="./")
        env = jinja2.Environment(loader=loader)

        template = env.get_template("update.sql.j2")
        rendered_query = template.render(
            dataset=self.dataset,
            table=self.table,
            p_key=",".join(self.key.get("p_key")),
            incremental_key=self.key.get("incremental_key"),
            partition_key=self.key.get("partition_key"),
        )

        _ = self.client.query(rendered_query).result()

    def run(self):
        """Main extract & load operation

        Returns:
            dict: Response for server
        """
        rows = self.fetch()
        rows = self.transform(rows)
        results = self.load(rows)
        self.update()

        return {
            "ads_account": self.ads_account,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "num_processed": self.num_processed,
            "output_rows": getattr(results, "output_rows", None),
            "errors": getattr(results, "errors", None),
        }


class FacebookAdsInsightsHourly(FacebookAdsInsights):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _init_table(self):
        return "AdsInsights_Hourly"

    def _init_config(self):
        with open(f"configs/config_Hourly.json", "r") as f:
            config = json.load(f)
        return config

    def _breakdowns_params_factory(self, params):
        params["level"] = "account"
        params["breakdowns"] = "hourly_stats_aggregated_by_advertiser_time_zone"
        return params

    def _schema_factory(self):
        with open("schemas/AdsInsights_Hourly.json") as f:
            schema = json.load(f)
        return schema


def facebook_ads_insights_factory(**kwargs):
    if "breakdowns" in kwargs:
        if kwargs["breakdowns"] == "hourly":
            return FacebookAdsInsightsHourly(**kwargs)
        else:
            return FacebookAdsInsights(**kwargs)
    else:
        return FacebookAdsInsights(**kwargs)


def main(event, context):
    data = event["data"]
    message = json.loads(base64.b64decode(data).decode("utf-8"))
    print(message)
    if "start_date" in message and "end_date" in message:
        job = facebook_ads_insights_factory(
            breakdowns=message.get("breakdowns"),
            start_date=message["start_date"],
            end_date=message["end_date"],
        )
    else:
        job = facebook_ads_insights_factory(breakdowns=message.get("breakdowns"))

    results = {
        "pipelines": "Facebook Ads Insights",
        "results": job.run(),
    }
    print(results)

    return results
