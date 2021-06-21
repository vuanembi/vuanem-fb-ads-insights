import json
import base64

import models
from broadcast import broadcast


def main(request):
    """Main function as gateway

    Args:
        event (dict): PubSubMessage
        context (google.cloud.functions.Context): Event Context

    Returns:
        dict: models results
    """
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if data:
        if "broadcast" in data:
            if data["broadcast"] in ["ads_insights", "ads_creatives"]:
                message_sent = broadcast(data["broadcast"])
            else:
                raise NotImplementedError(data["broadcast"])

            responses = {"message_sent": message_sent, "run": data["broadcast"]}
            print(responses)
            return responses
        if "ads_account_id" in data:
            if "mode" in data:
                jobs = [
                    models.AdsAPI.factory(
                        ads_account_id=data.get("ads_account_id"),
                        start=data.get("start"),
                        end=data.get("end"),
                        mode=data.get("mode"),
                    )
                ]
            else:
                jobs = [
                    models.AdsAPI.factory(
                        ads_account_id=data.get("ads_account_id"),
                        start=data.get("start"),
                        end=data.get("end"),
                        mode=i,
                    )
                    for i in [
                        "hourly",
                        "devices",
                        "country_region",
                        # "ads_creatives",
                    ]
                ]
            responses = {
                "pipelines": "Facebook Ads Insights",
                "results": [job.run() for job in jobs],
            }
            print(responses)

            return responses
        else:
            raise NotImplementedError
