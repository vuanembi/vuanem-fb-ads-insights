from facebook.pipeline.interface import AdsInsights


video_insights = AdsInsights(
    "VideoInsights",
    "ad",
    [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
        "adset_id",
        "ad_id",
        "campaign_name",
        "adset_name",
        "ad_name",
        "impressions",
        "clicks",
        "spend",
        "reach",
        "actions",
        "action_values",
    ],
    lambda rows: [
        {
            "account_id": row["account_id"],
            "date_start": row["date_start"],
            "date_stop": row["date_stop"],
            "campaign_id": row["campaign_id"],
            "adset_id": row["adset_id"],
            "ad_id": row["ad_id"],
            "campaign_name": row["campaign_name"],
            "adset_name": row["adset_name"],
            "ad_name": row["ad_name"],
            "impressions": row.get("impressions"),
            "reach": row.get("reach"),
            "spend": row.get("spend"),
            "clicks": row.get("clicks"),
            "video_asset": {
                "video_id": row["video_asset"].get("video_id"),
                "url": row["video_asset"].get("url"),
                "thumbnail_url": row["video_asset"].get("thumbnail_url"),
                "video_name": row["video_asset"].get("video_name"),
                "id": row["video_asset"].get("id"),
            }
            if row.get("video_asset", {})
            else {},
            "actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["actions"]
            ]
            if row.get("actions", [])
            else [],
            "action_values": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["action_values"]
            ]
            if row.get("action_values", [])
            else [],
        }
        for row in rows
    ],
    [
        {"name": "account_id", "type": "NUMERIC"},
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
        {"name": "campaign_id", "type": "NUMERIC"},
        {"name": "adset_id", "type": "NUMERIC"},
        {"name": "ad_id", "type": "NUMERIC"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "adset_name", "type": "STRING"},
        {"name": "ad_name", "type": "STRING"},
        {"name": "impressions", "type": "NUMERIC"},
        {"name": "clicks", "type": "NUMERIC"},
        {"name": "spend", "type": "NUMERIC"},
        {"name": "reach", "type": "NUMERIC"},
        {
            "name": "video_asset",
            "type": "RECORD",
            "fields": [
                {"name": "video_id", "type": "NUMERIC"},
                {"name": "url", "type": "STRING"},
                {"name": "thumbnail_url", "type": "STRING"},
                {"name": "video_name", "type": "STRING"},
                {"name": "id", "type": "NUMERIC"},
            ],
        },
        {
            "name": "actions",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
            ],
        },
        {
            "name": "action_values",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
            ],
        },
    ],
    [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
        "adset_id",
        "ad_id",
        "video_asset.video_id",
    ],
    "video_asset",
)
