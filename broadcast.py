import os
import json
from google.cloud import pubsub_v1

API_VER = os.getenv("API_VER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BASE_URL = f"https://graph.facebook.com/{API_VER}/"
BUSINESS_ID = "444284753088897"


def get_ads_accounts():
    """Get Ads Accounts ID

    Returns:
        list: List of ID
    """

    return [
        "act_808142069649310",
        "act_2419414334994459",
        "act_3921338037921594"
    ]


def broadcast(broadcast_mode):
    """Broadcast jobs to PubSub

    Args:
        broadcast_mode (str): Broadcast mode

    Returns:
        int: Number of messages
    """
    
    running_ads_accounts = get_ads_accounts()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))

    for ads_account_id in running_ads_accounts:
        data = {"ads_account_id": ads_account_id}
        if broadcast_mode == "ads_insights":
            data["mode"] = None
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return len(running_ads_accounts)
