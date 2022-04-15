from typing import Callable, Any
import os
import json
import uuid

from google.cloud import tasks_v2
from google import auth


_, PROJECT_ID = auth.default()
TASKS_CLIENT = tasks_v2.CloudTasksClient()

CLOUD_TASKS_PATH = (PROJECT_ID, "us-central1", "tms")


def create_tasks(
    payloads: list[dict[str, Any]],
    name_fn: Callable[[dict[str, Any]], str],
) -> int:
    tasks = [
        {
            "parent": TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH),
            "task": {
                "name": TASKS_CLIENT.task_path(
                    *CLOUD_TASKS_PATH,
                    task=f"{name_fn(payload)}-{uuid.uuid4()}",
                ),
                "http_request": {
                    "http_method": tasks_v2.HttpMethod.POST,
                    "url": os.getenv("PUBLIC_URL"),
                    "oidc_token": {
                        "service_account_email": os.getenv("GCP_SA"),
                    },
                    "headers": {
                        "Content-type": "application/json",
                    },
                    "body": json.dumps(payload).encode(),
                },
            },
        }
        for payload in payloads
    ]
    return len([TASKS_CLIENT.create_task(task) for task in tasks])
