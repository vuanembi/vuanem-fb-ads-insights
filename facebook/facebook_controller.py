from typing import Any

from facebook.pipeline import pipelines
from facebook.facebook_service import pipeline_service


def facebook_controller(body: dict[str, Any]):
    if body.get("table") in pipelines:
        return pipeline_service(pipelines[body["table"]])(
            body["ads_account_id"],
            body.get("start"),
            body.get("end"),
        )
