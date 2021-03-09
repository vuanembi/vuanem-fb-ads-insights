import os
import sys
import json
import asyncio
from datetime import datetime, timedelta

import requests
import aiohttp
from tqdm import tqdm
from google.cloud import bigquery

# To run asyncio on Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class FacebookAdsInsightsJob:
    def __init__(self, ads_account, table, **kwargs):
        """Init. Getting env & configs from envs"""

        self.ads_account = ads_account
        self.dataset = "Facebook"
        self.table = table

        self.access_token = os.getenv("ACCESS_TOKEN")
        self.api_ver = os.getenv("API_VER")

        with open("config.json", "r") as f:
            config = json.load(f)
        self.fields = config.get("fields")
        self.windows = config.get("action_attribution_windows")
        self.fields_with_windows = config.get("fields_with_windows")
        self.dict_windows = self.windows_map()

        if "end_date" in kwargs:
            self.manual = True
            self.end_date = datetime.strptime(kwargs["end_date"], "%Y-%m-%d")
        else:
            self.manual = False
            self.end_date = datetime.now()

        if "start_date" in kwargs:
            self.start_date = datetime.strptime(kwargs["start_date"], "%Y-%m-%d")
        else:
            self.start_date = datetime.now() - timedelta(days=30)

        self.num_processed = 0
        self.job_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def windows_map(self):
        """Change fields name to adhere to column name

        Returns:
            dict: Mapping
        """

        dict_windows = {}
        for field in self.windows:
            dict_windows[field] = "_" + field
        return dict_windows

    def generate_time_ranges(self):
        """Generate date ranges between two dates

        Returns:
            list: List of dates
        """

        date_ranges = [
            self.start_date + timedelta(n)
            for n in range(int((self.end_date - self.start_date).days) + 1)
        ]
        return list(map(lambda x: x.strftime("%Y-%m-%d"), date_ranges))

    def transform_result(self, result):
        """Transform fields name to adhere to columns' naming requirements

        Args:
            result (dict): Individual result

        Returns:
            dict: Processed individual result
        """

        for nest in self.fields_with_windows:
            actions = result.get(nest)
            if actions:
                for i in actions:
                    for k in list(i.keys()):
                        if k in self.dict_windows:
                            i["_" + k] = i.pop(k)
        return result

    async def fetch_one(self, sessions, dt):
        """Fetch one day of data

        Args:
            sessions (aiohttp.ClientSession): Client to requests API
            dt (str): Date in %Y-%m-%d
        """

        time_range = {"since": dt, "until": dt}
        params = {
            "access_token": self.access_token,
            "level": "ad",
            "time_range": json.dumps(time_range),
            "fields": json.dumps(self.fields),
            "action_attribution_windows": json.dumps(self.windows),
            "limit": 500,
        }
        async with sessions.get(
            "https://graph.facebook.com/{api_ver}/{ads_account}/insights".format(
                api_ver=self.api_ver,
                ads_account=self.ads_account,
            ),
            params=params,
        ) as r:
            assert r.status == 200
            response = await r.json()
        results = response.get("data")

        if len(results) > 0:
            self.num_processed += 1
            return list(map(self.transform_result, results))
        else:
            return None

    async def fetch_all(self):
        """Organize fetching all data in specified date ranges"""

        date_ranges = self.generate_time_ranges()
        async with aiohttp.ClientSession() as sessions:
            tasks = [
                asyncio.create_task(self.fetch_one(sessions, dt)) for dt in date_ranges
            ]
            rows = [
                await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))
            ]

        rows = [i for i in rows if i is not None]
        rows = [i for sublist in rows for i in sublist]
        return [dict(item, **{"_batched_at": self.job_ts}) for item in rows]

    def run(self):
        """Main extract & load operation

        Returns:
            dict: Response for server
        """
        client = bigquery.Client()
        rows = asyncio.run(self.fetch_all())

        with open("schemas/AdsInsights.json") as f:
            schema = json.load(f)

        load_to_stage_job = client.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

        return {
            "ads_account": self.ads_account,
            "start_date": self.start_date.strftime("%Y-%m-%d"),
            "end_date": self.end_date.strftime("%Y-%m-%d"),
            "num_processed": self.num_processed,
            "output_rows": load_to_stage_job.output_rows,
            "load_errors": load_to_stage_job.errors,
        }


def main(request):
    VuaNemTk01 = FacebookAdsInsightsJob(
        "act_2419414334994459", "Ads_Insights_VuaNemTk01"
    )
    VuaNemUSD = FacebookAdsInsightsJob("act_808142069649310", "Ads_Insights_VuaNemUSD")

    results = {
        "pipelines": "Facebook Ads Insights",
        "results": [VuaNemTk01.run(), VuaNemUSD.run()],
    }
    print(results)

    _ = requests.post(
        "https://api.telegram.org/bot{token}/sendMessage".format(
            token=os.getenv("TELEGRAM_TOKEN")
        ),
        json={
            "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
            "text": json.dumps(results, indent=4),
        },
    )

    return results
