import os
import sys
import json
import asyncio
from datetime import datetime, timedelta

import aiohttp
from aiohttp.connector import TCPConnector
from gcloud.aio.storage import Storage

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class FacebookAdsInsightsJob:
    def __init__(self):
        self.access_token = os.getenv("ACCESS_TOKEN")
        self.ads_account = os.getenv("ADS_ACCOUNT")
        self.api_ver = os.getenv("API_VER")
        self.bucket = os.getenv("BUCKET")
        self.path = os.getenv("PATH")

        with open("config.json", "r") as f:
            config = json.load(f)

        self.fields = config.get("fields")
        self.windows = config.get("action_attribution_windows")
        self.dict_windows = self.windows_map()

    def windows_map(self):
        dict_windows = {}
        for field in self.windows:
            dict_windows[field] = "_" + field
        return dict_windows

    def generate_time_ranges(self):
        today = datetime.now()
        start_date = today - timedelta(days=30)
        date_ranges = [
            start_date + timedelta(n) for n in range(int((today - start_date).days) + 1)
        ]
        return list(map(lambda x: x.strftime("%Y-%m-%d"), date_ranges))

    def transform_result(self, result):
        for nest in ["actions", "action_values"]:
            actions = result.get(nest)
            if actions:
                for i in actions:
                    for k in list(i.keys()):
                        if k in self.dict_windows:
                            i["_" + k] = i.pop(k)
        return result

    async def fetch_one(self, sessions, storage_client, dt):
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
            response = await r.json()
        results = response.get("data", [])
        results = list(map(self.transform_result, results))
        results = "\n".join([json.dumps(result) for result in results])
        print(results)
        _ = await storage_client.upload(
            self.bucket,
            self.path + dt + ".json",
            results,
            timeout=60,
        )

    async def fetch_all(self):
        date_ranges = self.generate_time_ranges()
        async with aiohttp.ClientSession() as sessions, Storage(
            session=aiohttp.ClientSession()
        ) as storage_client:
            tasks = [
                asyncio.create_task(
                    self.fetch_one(sessions, storage_client, dt)
                ) for dt in date_ranges
            ]
            _ = await asyncio.gather(*tasks)

    def run(self):
        asyncio.run(self.fetch_all())

def main(request):
    job = FacebookAdsInsightsJob()
    _ = job.run()
