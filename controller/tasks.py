import os
import json
import uuid

from google.cloud import tasks_v2
from google import auth

SERVICE_ACCOUNT, PROJECT_ID = auth.default()
TASKS_CLIENT = tasks_v2.CloudTasksClient()

TABLES = [
    "AdsInsights",
    "AdsInsights_Region",
]

ACCOUNTS = [
    {
        "client": "VuaNemUSD",
        "ads_account_id": "act_808142069649310",
    },
    {
        "client": "VuaNemTK01",
        "ads_account_id": "act_2419414334994459",
    },
    {
        "client": "NovaOn",
        "ads_account_id": "act_3921338037921594",
    },
]

CLOUD_TASKS_PATH = (PROJECT_ID, "us-central1", "fb-ads-insights")
PARENT = TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH)


def create_tasks(tasks_data: dict) -> dict:
    payloads = [
        {
            "name": f"{account['client']}-{uuid.uuid4()}",
            "payload": {
                "table": tasks_data["table"],
                "ads_account_id": account["ads_account_id"],
                "start": tasks_data.get("start"),
                "end": tasks_data.get("end"),
            },
        }
        for account in ACCOUNTS
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(
                *CLOUD_TASKS_PATH,
                task=str(payload["name"]),
            ),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": os.getenv("PUBLIC_URL"),
                "oidc_token": {
                    "service_account_email": SERVICE_ACCOUNT.service_account_email
                },
                "headers": {"Content-type": "application/json"},
                "body": json.dumps(payload["payload"]).encode(),
            },
        }
        for payload in payloads
    ]
    return {
        "tasks": len(
            [
                TASKS_CLIENT.create_task(
                    request={
                        "parent": PARENT,
                        "task": task,
                    }
                )
                for task in tasks
            ]
        ),
        "tasks_data": tasks_data,
    }
