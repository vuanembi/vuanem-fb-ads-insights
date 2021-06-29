import os
import json
import time
from datetime import datetime
from urllib3.util.retry import Retry
from abc import abstractmethod, ABCMeta
import asyncio
import re


import requests
from requests.adapters import HTTPAdapter
import aiohttp
from google.cloud import bigquery
import jinja2

API_VER = os.getenv("API_VER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BASE_URL = f"https://graph.facebook.com/{API_VER}/"
BUSINESS_ID = "444284753088897"
DATASET = "Facebook"

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)
BQ_CLIENT = bigquery.Client()

class AdsAPI(metaclass=ABCMeta):
    def __init__(self, ads_account_id, start, end, mode):
        """Init. Getting env & configs from envs"""

        self.session = self._init_session()
        self.ads_account_id = ads_account_id
        self.num_processed = 0
        self.job_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.table = self._init_table()
        self.start, self.end, self.date_preset = self.init_time_range(start, end)
        self.init_config()
        
        self.mode = mode

    @staticmethod
    def factory(ads_account_id, start=None, end=None, mode=None):
        """Factory Method to create Facebook Ads Pipelines

        Args:
            ads_account_id (str): Ads Account ID
            start (str, optional): Date in %Y-%m-%d. Defaults to None.
            end (str, optional): Date in %Y-%m-%d. Defaults to None.
            mode (str, optional): Mode to run. Defaults to None.

        Returns:
            AdsAPI: Pipelines
        """

        if mode is not None:
            if mode == "hourly":
                return AdsInsightsHourly(ads_account_id, start, end, mode)
            elif mode == "age_genders":
                return AdsInsightsAgeGenders(ads_account_id, start, end, mode)
            elif mode == "devices":
                return AdsInsightsDevices(ads_account_id, start, end, mode)
            elif mode == "country_region":
                return AdsInsightsCountryRegion(ads_account_id, start, end, mode)
            elif mode == "ads_creatives":
                return AdsCreatives(ads_account_id, start, end, mode)
        else:
            return AdsInsightsStandard(ads_account_id, start, end, mode)

    def init_config(self):
        """Initialize config from json"""

        config = self._init_config()
        self.field = config.get("field")
        self.fields = self.field.get("fields")
        self.windows = self.field.get("action_attribution_windows")
        self.fields_with_windows = self.field.get("fields_with_windows")
        self.key = config.get("key")

    @abstractmethod
    def _init_config(self):
        """Get config file

        Returns:
            dict: Configs
        """

        raise NotImplementedError

    def init_time_range(self, start, end):
        """Get time range to get data

        Args:
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d

        Returns:
            tuple: (start, end, date_preset)
        """
                
        if start and end:
            return start, end, None
        else:
            date_preset = "last_28d"
            return date_preset, date_preset, date_preset

    def _init_session(self):
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
    def _init_table(self):
        """Initiate table name

        Returns:
            str: Table Name
        """

        raise NotImplementedError

    def get_ads_account_name(self):
        """Get Ads Account Name to determine dataset

        Raises:
            e: Exception

        Returns:
            str: Ads Account Name
        """

        url = f"{BASE_URL}{self.ads_account_id}"
        try:
            with self.session.get(
                url, params={"access_token": ACCESS_TOKEN, "fields": "name"}
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
    def fetch(self):
        """Abstract Method to fetch Data

        Returns:
            list: List of dicts
        """

        raise NotImplementedError

    @abstractmethod
    def transform(self, rows):
        """Abstract Method to transform data

        Args:
            rows (list): List of dicts

        Raises:
            list: List of dicts
        """

        raise NotImplementedError

    def load(self, rows):
        """Load to BigQuery with predetermined schema to staging table

        Args:
            rows (list): List of results as JSON

        Returns:
            google.cloud.bigquery.job.base_AsyncJob: LoadJob Results
        """

        schema = self._schema_factory()

        loads = BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()
        return loads

    @abstractmethod
    def _schema_factory(self):
        """Get the Schema for corresponding pipelines breakdowns

        Returns:
            list: BigQuery JSON Schema
        """

        raise NotImplementedError

    def update(self):
        """Update the main table using the staging table"""

        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.key.get("p_key")),
            incremental_key=self.key.get("incremental_key"),
        )

        _ = BQ_CLIENT.query(rendered_query).result()

    def run(self):
        """Main extract & load operation

        Returns:
            dict: Job Results
        """

        rows = self.fetch()
        if len(rows) > 0:
            rows = self.transform(rows)
            results = self.load(rows)
            self.update()

            return {
                "ads_account": self.ads_account_id,
                "dataset": DATASET,
                "mode": self.mode,
                "start_date": self.start,
                "end_date": self.end,
                "num_processed": self.num_processed,
                "output_rows": getattr(results, "output_rows", None),
                "errors": getattr(results, "errors", None),
            }
        else:
            return {
                "ads_account": self.ads_account_id,
                "dataset": DATASET,
                "mode": self.mode,
                "start_date": self.start,
                "end_date": self.end
            }


class AdsInsights(AdsAPI):
    def __init__(self, ads_account_id, start, end, mode):
        super().__init__(ads_account_id, start, end, mode)

    def init_config(self):
        config = self._init_config()
        self.field = config.get("field")
        self.fields = self.field.get("fields")
        self.windows = self.field.get("action_attribution_windows")
        self.fields_with_windows = self.field.get("fields_with_windows")
        self.key = config.get("key")

    @abstractmethod
    def _init_config(self):
        raise NotImplementedError

    def fetch(self):
        """Fetch Facebook API

        Returns:
            self._fetch_insights: Function for breakdowns
        """

        report_run_id = self._fetch_report_id()
        report_run_id = self._poll_report_id(report_run_id)
        return self._fetch_insights(report_run_id)

    @abstractmethod
    def _breakdowns_params_factory(self, params):
        """Generate parameters for API queries

        Args:
            params (dict): Parameters

        Returns:
            dict: Parameters
        """

        raise NotImplementedError

    def _fetch_report_id(self):
        """Requests an Async Facebook API Job

        Returns:
            str: Async Facebook API Job
        """

        url = BASE_URL + f"{self.ads_account_id}/insights"

        params = {
            "access_token": ACCESS_TOKEN,
            "level": "ad",
            "filtering": '[{field:"ad.impressions",operator:"GREATER_THAN",value:0},]',
            "fields": json.dumps(self.fields),
            "action_attribution_windows": json.dumps(self.windows),
            "time_increment": 1,
        }
        if self.date_preset is None:
            time_range = {"since": self.start, "until": self.end}
            params["time_range"] = json.dumps(time_range)
        else:
            params["date_preset"] = self.date_preset
        params = self._breakdowns_params_factory(params)

        try:
            with self.session.post(url, params=params) as r:
                r.raise_for_status()
                res = r.json()
            return res["report_run_id"]
        except requests.exceptions.HTTPError as e:
            print(e.response.text)
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

        url = BASE_URL + report_run_id
        while True:
            params = {"access_token": ACCESS_TOKEN}
            try:
                with self.session.get(url, params=params) as r:
                    res = r.json()
            except requests.exceptions.HTTPError as e:
                print(e.response.text)
                raise e
            if (res["async_status"] == "Job Completed") and (
                res["async_percent_completion"] == 100
            ):
                return report_run_id
            elif res["async_status"] in ["Job Failed", "Job Skipped"]:
                raise requests.exceptions.HTTPError(res)
            else:
                time.sleep(5)

    def _fetch_insights(self, report_run_id):
        """Fetch Faceboo API insights corresponding to breakdowns

        Args:
            report_run_id (str): Async Facebook API Job

        Returns:
            list: List of results as JSON
        """

        url = BASE_URL + f"{report_run_id}/insights"
        rows = []
        after = None
        while True:
            params = {"access_token": ACCESS_TOKEN, "limit": 500}
            if after:
                params["after"] = after
            try:
                url
                with self.session.get(url, params=params) as r:
                    r.raise_for_status()
                    res = r.json()
            except requests.exceptions.HTTPError as e:
                print(e.response.text)
                raise e
            data = res.get("data")
            rows.extend(data)
            next = res.get("paging").get("next")
            if next:
                after = res.get("paging").get("cursors").get("after")
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

    @abstractmethod
    def _schema_factory(self):
        """Get the Schema for corresponding pipelines breakdowns

        Returns:
            list: BigQuery JSON Schema
        """

        raise NotImplementedError


class AdsInsightsStandard(AdsInsights):
    def __init__(self, ads_account_id, start, end, mode):
        super().__init__(ads_account_id, start, end, mode)

    def _init_config(self):
        with open(f"configs/config.json", "r") as f:
            config = json.load(f)
        return config

    def _init_table(self):
        name = self.get_ads_account_name()
        return f"{name}_AdsInsights"

    def _breakdowns_params_factory(self, params):
        return params

    def _schema_factory(self):
        with open("schemas/AdsInsights.json") as f:
            schema = json.load(f)
        return schema


class AdsInsightsHourly(AdsInsights):
    def __init__(self, ads_account_id, start, end, mode):
        super().__init__(ads_account_id, start, end, mode)

    def _init_table(self):
        name = self.get_ads_account_name()
        return f"{name}_AdsInsights_Hourly"

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


class AdsInsightsAgeGenders(AdsInsights):
    def __init__(self, ads_account_id, start, end, mode):
        super().__init__(ads_account_id, start, end, mode)

    def _init_table(self):
        return "AdsInsights_AgeGenders"

    def _init_config(self):
        with open(f"configs/config_AgeGenders.json", "r") as f:
            config = json.load(f)
        return config

    def _breakdowns_params_factory(self, params):
        params["level"] = "account"
        breakdowns = ["age", "gender"]
        params["breakdowns"] = ",".join(breakdowns)
        return params

    def _schema_factory(self):
        with open("schemas/AdsInsights_AgeGenders.json") as f:
            schema = json.load(f)
        return schema


class AdsInsightsDevices(AdsInsights):
    def __init__(self, ads_account_id, start, end, mode):
        super().__init__(ads_account_id, start, end, mode)

    def _init_table(self):
        name = self.get_ads_account_name()
        return f"{name}_AdsInsights_Devices"

    def _init_config(self):
        with open(f"configs/config_Devices.json", "r") as f:
            config = json.load(f)
        return config

    def _breakdowns_params_factory(self, params):
        params["level"] = "account"
        breakdowns = ["device_platform"]
        params["breakdowns"] = ",".join(breakdowns)
        return params

    def _schema_factory(self):
        with open("schemas/AdsInsights_Devices.json") as f:
            schema = json.load(f)
        return schema


class AdsInsightsCountryRegion(AdsInsights):
    def __init__(self, ads_account_id, start, end, mode):
        super().__init__(ads_account_id, start, end, mode)

    def _init_table(self):
        name = self.get_ads_account_name()
        return f"{name}_AdsInsights_CountryRegion"

    def _init_config(self):
        with open(f"configs/config_CountryRegion.json", "r") as f:
            config = json.load(f)
        return config

    def _breakdowns_params_factory(self, params):
        params["level"] = "account"
        breakdowns = ["country", "region"]
        params["breakdowns"] = ",".join(breakdowns)
        return params

    def _schema_factory(self):
        with open("schemas/AdsInsights_CountryRegion.json") as f:
            schema = json.load(f)
        return schema


class AdsCreatives(AdsAPI):
    def __init__(self, ads_account_id, start, end, mode):
        super().__init__(ads_account_id, start, end, mode)

    def init_config(self):
        config = self._init_config()
        self.field = config.get("field")
        self.fields = self.field.get("fields")
        self.key = config.get("key")

    def _init_config(self):
        with open(f"configs/config_AdsCreatives.json", "r") as f:
            config = json.load(f)
        return config

    def _init_table(self):
        name = self.get_ads_account_name()
        return f"{name}_AdsCreatives"

    def _fetch_ad_ids(self):
        template = TEMPLATE_ENV.get_template("read_latest_ids.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            id_key="ad_id",
        )

        rows = BQ_CLIENT.query(rendered_query).result()
        rows = [row["ad_id"] for row in [dict(row.items()) for row in rows]]
        return rows

    def fetch(self):
        return asyncio.run(self._fetch_wrapper())

    async def _fetch_wrapper(self):
        ad_ids = self._fetch_ad_ids()
        print("ads_ids", len(ad_ids))
        if len(ad_ids) > 0:
            connector = aiohttp.TCPConnector(limit=5)
            async with aiohttp.ClientSession(connector=connector) as session:
                ads_tasks = [
                    asyncio.create_task(self._fetch_ads(session, ad_id)) for ad_id in ad_ids
                ]
                ads = await asyncio.gather(*ads_tasks)
                ads_creatives_tasks = [
                    asyncio.create_task(
                        self._fetch_ads_creatives(session, ad["id"], ad["creative"])
                    )
                    for ad in ads
                ]
                ads_creatives = await asyncio.gather(*ads_creatives_tasks)
                self.num_processed = len(ads_creatives)
                return ads_creatives
        else:
            return []

    async def _fetch_ads(self, session, ad_id):
        url = BASE_URL + f"{ad_id}"
        params = {
            "access_token": ACCESS_TOKEN,
            "fields": json.dumps(["id", "creative"]),
        }
        try:
            async with session.get(url, params=params) as r:
                res = await r.json()
        except aiohttp.ClientResponseError as e:
            print(e.response.text)
            raise e
        return {"id": res["id"], "creative": res["creative"]["id"]}

    async def _fetch_ads_creatives(self, session, id, creative):
        url = BASE_URL + f"{creative}"
        params = {
            "access_token": ACCESS_TOKEN,
            "limit": 500,
            "fields": json.dumps(self.fields),
        }
        try:
            async with session.get(url, params=params) as r:
                res = await r.json()
        except aiohttp.ClientResponseError as e:
            print(e.response.text)
            raise e
        return {**res, "ad_id": id}

    def transform(self, rows):
        rows = [dict(row, **{"_batched_at": self.job_ts}) for row in rows]
        self.num_processed = len(rows)
        return rows

    def _schema_factory(self):
        with open("schemas/AdsCreatives.json") as f:
            schema = json.load(f)
        return schema
