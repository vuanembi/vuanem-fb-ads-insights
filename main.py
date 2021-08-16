import json
import base64

import models
from broadcast import broadcast


def main(request):
    """Main function as gateway

    Args:
        request (flask.Request): HTTP request 

    Returns:
        dict: Responses
    """

    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if data:
        if "broadcast" in data:
            if data["broadcast"] in [
                "standard",
                "ads_creatives",
                "misc",
            ]:
                message_sent = broadcast(data)
            else:
                raise NotImplementedError(data)

            results = {"message_sent": message_sent, "run": data["broadcast"]}
        elif "ads_account_id" in data and "broadcast" not in data:
            if "mode" == "misc":
                jobs = [
                    models.AdsAPI.factory(
                        ads_account_id=data.get("ads_account_id"),
                        start=data.get("start"),
                        end=data.get("end"),
                        mode=i,
                    )
                    for i in [
                        # "hourly",
                        # "devices",
                        "country_region",
                        # "age_genders",
                    ]
                ]

            else:
                jobs = [
                    models.AdsAPI.factory(
                        ads_account_id=data.get("ads_account_id"),
                        start=data.get("start"),
                        end=data.get("end"),
                        mode=data.get("mode"),
                    )
                ]
            results = [job.run() for job in jobs]
        else:
            raise NotImplementedError(data)
            
        responses = {
            "pipelines": "Facebook Ads Insights",
            "results": results,
        }
        print(responses)

        return responses
    else:
        raise NotImplementedError
