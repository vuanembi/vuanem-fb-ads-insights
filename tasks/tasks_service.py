from facebook.pipeline import pipelines
from tasks import cloud_tasks

ACCOUNTS = [
    # "808142069649310", # "VuaNemUSD
    # "2419414334994459", # "VuaNemTK01
    "3921338037921594", # NovaOn
    "408351477456855", # "DiamondVuaNemLZD
    "1082729769139634", # "DiamondVuaNemShopee
    "796051681037501", # "DiamondVuaNemTiki
]


def tasks_service(body: dict[str, str]):
    return {
        "tasks": cloud_tasks.create_tasks(
            [
                {
                    "table": i,
                    "ads_account_id": a,
                    "start": body.get("start"),
                    "end": body.get("end"),
                }
                for i in pipelines.keys()
                for a in i
            ],
            lambda x: x["table"],
        )
    }
