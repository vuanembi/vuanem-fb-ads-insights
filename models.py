import os
import json
import time
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from abc import abstractmethod, ABCMeta
import asyncio
import re


import requests
from requests.adapters import HTTPAdapter
import aiohttp
from google.cloud import bigquery
import jinja2

DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
NOW = datetime.now()

API_VER = os.getenv("API_VER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BASE_URL = f"https://graph.facebook.com/{API_VER}"

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

BQ_CLIENT = bigquery.Client()
DATASET = "Facebook"


class AdsAPI(metaclass=ABCMeta):
    def __init__(self, ads_account_id, start, end):
        """Init. Getting env & configs from envs"""

        self.ads_account_id = ads_account_id
        self.session = self.get_sessions()
        self.table = self._get_table()
        self.keys, self.fields, self.schema = self.get_config()
        self.start, self.end = self.get_time_range(start, end)
        self.ads_account_name = self.get_ads_account_name()

    @staticmethod
    def factory(ads_account_id, start, end, mode):
        """Factory Method to create Facebook Ads Pipelines

        Args:
            ads_account_id (str): Ads Account ID
            start (str, optional): Date in %Y-%m-%d. Defaults to None.
            end (str, optional): Date in %Y-%m-%d. Defaults to None.
            mode (str, optional): Mode to run. Defaults to None.

        Returns:
            AdsAPI: Pipelines
        """

        args = (ads_account_id, start, end)
        if mode == "standard":
            return AdsInsightsStandard(*args)
        elif mode == "hourly":
            return AdsInsightsHourly(*args)
        elif mode == "age_genders":
            return AdsInsightsAgeGenders(*args)
        elif mode == "devices":
            return AdsInsightsDevices(*args)
        elif mode == "country_region":
            return AdsInsightsCountryRegion(*args)
        elif mode == "ads_creatives":
            return AdsCreatives(*args)
        else:
            raise NotImplementedError(*args)

    def get_config(self):
        """Get config from JSON

        Returns:
            tuple: (keys, fields, schema)
        """

        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config.get("keys"), config.get("fields"), config.get("schema")

    def get_time_range(self, _start, _end):
        """Get time range

        Args:
            _start (str): Date in %Y-%m-%d
            _end (str): Date in %Y-%m-%d

        Returns:
            tuple: (start, end)
        """

        if _start and _end:
            start = _start
            end = _end
        else:
            start = (NOW - timedelta(days=28)).strftime(DATE_FORMAT)
            end = NOW.strftime(DATE_FORMAT)
        return start, end

    def get_sessions(self):
        """Initiate HTTP session for requests

        Returns:
            requests.Session: HTTP session
        """

        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 503, 500],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    @abstractmethod
    def _get_table(self):
        """Initiate table name

        Returns:
            str: Table Name
        """

        pass

    def get_ads_account_name(self):
        """Get Ads Account Name to determine dataset

        Raises:
            e: Exception

        Returns:
            str: Ads Account Name
        """

        url = f"{BASE_URL}/{self.ads_account_id}"
        try:
            with self.session.get(
                url,
                params={
                    "access_token": ACCESS_TOKEN,
                    "fields": "name",
                },
            ) as r:
                r.raise_for_status()
                res = r.json()
        except requests.exceptions.HTTPError as e:
            print(e.response.text)
            raise e
        pattern = "[^0-9a-zA-Z]+"
        name = re.sub(pattern, "", res["name"])
        return name

    @abstractmethod
    def get(self):
        """Abstract Method to get Data

        Returns:
            list: List of dicts
        """

        pass

    @abstractmethod
    def transform(self, rows):
        """Abstract Method to transform data

        Args:
            rows (list): List of dicts

        Returns:
            list: List of dicts
        """

        pass

    def load(self, rows):
        """Load to BigQuery with predetermined schema to staging table

        Args:
            rows (list): List of results as JSON

        Returns:
            google.cloud.bigquery.job.base_AsyncJob: LoadJob Results
        """

        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.ads_account_name}_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

    def update(self):
        """Update the main table using the staging table"""

        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=f"{self.ads_account_name}_{self.table}",
            p_key=",".join(self.keys.get("p_key")),
            incremental_key=self.keys.get("incre_key"),
        )

        BQ_CLIENT.query(rendered_query).result()

    def run(self):
        """Main extract & load operation

        Returns:
            dict: Job Results
        """

        rows = self.get()
        responses = {
            "ads_account": self.ads_account_id,
            "dataset": DATASET,
            "table": self.table,
            "start_date": self.start,
            "end_date": self.end,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses["output_rows"] = loads.output_rows
        return responses


class AdsInsights(AdsAPI):
    def __init__(self, ads_account_id, start, end):
        super().__init__(ads_account_id, start, end)

    def get(self):
        """get Facebook API

        Returns:
            self._get_insights: Function for breakdowns
        """

        report_run_id = self._get_report_id()
        report_run_id = self._poll_report_id(report_run_id)
        return self._get_insights(report_run_id)

    @abstractmethod
    def _get_breakdowns(self, params):
        """Generate parameters for API queries

        Args:
            params (dict): Parameters

        Returns:
            dict: Parameters
        """

        pass

    def _get_report_id(self):
        """Requests an Async Facebook API Job

        Returns:
            str: Async Facebook API Job
        """

        url = f"{BASE_URL}/{self.ads_account_id}/insights"

        params = {
            "access_token": ACCESS_TOKEN,
            "level": "ad",
            "fields": json.dumps(self.fields["fields"]),
            "action_attribution_windows": json.dumps(
                [
                    "1d_click",
                    "1d_view",
                    "7d_click",
                    "7d_view",
                    "28d_click",
                    "28d_view",
                ]
            ),
            "time_range": json.dumps(
                {
                    "since": self.start,
                    "until": self.end,
                }
            ),
            "time_increment": 1,
        }
        params = self._get_breakdowns(params)

        try:
            with self.session.post(url, params=params) as r:
                r.raise_for_status()
                res = r.json()
            return res["report_run_id"]
        except requests.exceptions.HTTPError as e:
            print(e)
            raise e

    def _poll_report_id(self, report_run_id):
        """Poll the Async Facebook API job until it is completed

        Args:
            report_run_id (str): Async Facebook API Job

        Raises:
            RuntimeError: When job is failed

        Returns:
            str: Async Facebook API Job
        """

        url = f"{BASE_URL}/{report_run_id}"
        while True:
            params = {
                "access_token": ACCESS_TOKEN,
            }
            try:
                with self.session.get(
                    url,
                    params=params,
                ) as r:
                    r.raise_for_status()
                    res = r.json()
            except requests.exceptions.HTTPError as e:
                print(e.response.text)
                raise e
            if (res["async_status"] == "Job Completed") and (
                res["async_percent_completion"] == 100
            ):
                return report_run_id
            elif res["async_status"] in [
                "Job Failed",
                "Job Skipped",
            ]:
                raise requests.exceptions.HTTPError(res)
            else:
                time.sleep(10)

    def _get_insights(self, report_run_id):
        """get Faceboo API insights corresponding to breakdowns

        Args:
            report_run_id (str): Async Facebook API Job

        Returns:
            list: List of results as JSON
        """

        url = f"{BASE_URL}/{report_run_id}/insights"
        rows = []
        params = {
            "access_token": ACCESS_TOKEN,
            "limit": 500,
        }
        while True:
            try:
                with self.session.get(url, params=params) as r:
                    r.raise_for_status()
                    res = r.json()
            except requests.exceptions.HTTPError as e:
                print(e)
                raise e
            data = res.get("data")
            rows.extend(data)
            next = res.get("paging").get("next")
            if next:
                params["after"] = res.get("paging").get("cursors").get("after")
            else:
                break
        return rows

    def transform(self, rows):
        """Transform the rows with fixing prefix for attribution windows fields & adding _batched_at for querying the latest data

        Args:
            rows (list): List of results as JSON

        Returns:
            list: List of results as JSON
        """

        rows = [self._transform_prefix(row) for row in rows]
        rows = [
            dict(
                row,
                **{"_batched_at": NOW.strftime(TIMESTAMP_FORMAT)},
            )
            for row in rows
        ]
        return rows

    def _transform_prefix(self, row):
        """Transform fields name to adhere to columns' naming requirements

        Args:
            result (dict): Individual result

        Returns:
            dict: Processed individual result
        """

        for nest in self.fields.get("fields_with_windows"):
            fields = row.get(nest)
            if fields:
                for i in fields:
                    for k in [
                        j
                        for j in i.keys()
                        if j
                        not in [
                            "value",
                            "action_type",
                        ]
                    ]:
                        i["_" + k] = i.pop(k)
        return row


class AdsInsightsStandard(AdsInsights):
    def __init__(self, ads_account_id, start, end):
        super().__init__(ads_account_id, start, end)

    def _get_table(self):
        return "AdsInsights"

    def _get_breakdowns(self, params):
        params["filtering"] = json.dumps(
            [
                {"field": "ad.impressions", "operator": "GREATER_THAN", "value": 0},
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
        )
        return params


class AdsInsightsHourly(AdsInsights):
    def __init__(self, ads_account_id, start, end):
        super().__init__(ads_account_id, start, end)

    def _get_table(self):
        return "AdsInsights_Hourly"

    def _get_breakdowns(self, params):
        params["level"] = "account"
        params["breakdowns"] = "hourly_stats_aggregated_by_advertiser_time_zone"
        return params


class AdsInsightsAgeGenders(AdsInsights):
    def __init__(self, ads_account_id, start, end):
        super().__init__(ads_account_id, start, end)

    def _get_table(self):
        return "AdsInsights_AgeGenders"

    def _get_breakdowns(self, params):
        params["level"] = "account"
        breakdowns = ["age", "gender"]
        params["breakdowns"] = ",".join(breakdowns)
        return params


class AdsInsightsDevices(AdsInsights):
    def __init__(self, ads_account_id, start, end):
        super().__init__(ads_account_id, start, end)

    def _get_table(self):
        return "AdsInsights_Devices"

    def _get_breakdowns(self, params):
        params["level"] = "account"
        breakdowns = ["device_platform"]
        params["breakdowns"] = ",".join(breakdowns)
        return params


class AdsInsightsCountryRegion(AdsInsights):
    def __init__(self, ads_account_id, start, end):
        super().__init__(ads_account_id, start, end)

    def _get_table(self):
        return "AdsInsights_CountryRegion"

    def _get_breakdowns(self, params):
        params["level"] = "account"
        breakdowns = ["region"]
        params["breakdowns"] = ",".join(breakdowns)
        return params


class AdsCreatives(AdsAPI):
    def __init__(self, ads_account_id):
        super().__init__(ads_account_id)

    def _get_table(self):
        return "AdsCreatives"

    def _get_ad_ids(self):
        template = TEMPLATE_ENV.get_template("read_latest_ids.sql.j2")
        initial = False
        attempts = 0
        while attempts < 2:
            rendered_query = template.render(
                dataset=self.dataset, id_key="ad_id", initial=initial
            )
            try:
                rows = BQ_CLIENT.query(rendered_query).result()
                rows = [row["ad_id"] for row in [dict(row.items()) for row in rows]]
                return rows
            except:
                initial = True
                attempts += 1

    def get(self):
        return asyncio.run(self._get_wrapper())

    async def _get_wrapper(self):
        ad_ids = self._get_ad_ids()
        print("ads_ids", len(ad_ids))
        if len(ad_ids) > 0:
            connector = aiohttp.TCPConnector(limit=5)
            async with aiohttp.ClientSession(connector=connector) as session:
                ads_tasks = [
                    asyncio.create_task(self._get_ads(session, ad_id))
                    for ad_id in ad_ids
                ]
                ads = await asyncio.gather(*ads_tasks)
                ads_creatives_tasks = [
                    asyncio.create_task(
                        self._get_ads_creatives(session, ad["id"], ad["creative"])
                    )
                    for ad in ads
                ]
                ads_creatives = await asyncio.gather(*ads_creatives_tasks)
                self.num_processed = len(ads_creatives)
                return ads_creatives
        else:
            return []

    async def _get_ads(self, session, ad_id):
        url = f"{BASE_URL}/{ad_id}"
        params = {
            "access_token": ACCESS_TOKEN,
            "fields": json.dumps(["id", "creative"]),
        }
        try:
            async with session.get(
                url,
                params=params,
            ) as r:
                res = await r.json()
        except aiohttp.ClientResponseError as e:
            print(e.response.text)
            raise e
        return {
            "id": res["id"],
            "creative": res["creative"]["id"],
        }

    async def _get_ads_creatives(self, session, id, creative):
        url = f"{BASE_URL}/{creative}"
        params = {
            "access_token": ACCESS_TOKEN,
            "fields": ",".join(self.fields),
        }
        try:
            async with session.get(
                url,
                params=params,
            ) as r:
                r.raise_for_status()
                res = await r.json()
        except aiohttp.ClientResponseError as e:
            print(e)
            raise e
        return {**res, "ad_id": id}

    def transform(self, rows):
        rows = [
            dict(
                row,
                **{
                    "_batched_at": self.now.strftime(TIMESTAMP_FORMAT),
                },
            )
            for row in rows
        ]
        rows = [self._transform_dumps(row) for row in rows]
        self.num_processed = len(rows)
        return rows

    def _transform_dumps(self, row):
        for i in [
            "asset_feed_spec",
            "object_story_spec",
        ]:
            if i in row:
                row[i] = json.dumps(row[i])
        return row
