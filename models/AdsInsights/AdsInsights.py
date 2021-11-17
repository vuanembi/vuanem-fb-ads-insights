from models.AdsInsights.base import FBAdsInsights


AdsInsights: FBAdsInsights = {
    "name": "AdsInsights",
    "fields": [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
        "adset_id",
        "ad_id",
        "campaign_name",
        "adset_name",
        "ad_name",
        "reach",
        "impressions",
        "cpc",
        "cpm",
        "ctr",
        "clicks",
        "spend",
        "actions",
        "action_values",
        "cost_per_action_type",
        "cost_per_unique_action_type",
    ],
    "level": "ad",
    "breakdowns": None,
    "transform": lambda rows: [
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
            "reach": row.get("reach"),
            "impressions": row.get("impressions"),
            "cpc": row.get("cpc"),
            "cpm": row.get("cpm"),
            "ctr": row.get("ctr"),
            "clicks": row.get("clicks"),
            "spend": row.get('spend'),
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
            "cost_per_action_type": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["cost_per_action_type"]
            ]
            if row.get("cost_per_action_type", [])
            else [],
            "cost_per_unique_action_type": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["cost_per_unique_action_type"]
            ]
            if row.get("cost_per_unique_action_type", [])
            else [],
        }
        for row in rows
    ],
    "schema": [
        {"name": "account_id", "type": "INTEGER"},
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
        {"name": "campaign_id", "type": "INTEGER"},
        {"name": "adset_id", "type": "INTEGER"},
        {"name": "ad_id", "type": "INTEGER"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "adset_name", "type": "STRING"},
        {"name": "ad_name", "type": "STRING"},
        {"name": "reach", "type": "INTEGER"},
        {"name": "impressions", "type": "INTEGER"},
        {"name": "cpc", "type": "FLOAT"},
        {"name": "cpm", "type": "FLOAT"},
        {"name": "ctr", "type": "FLOAT"},
        {"name": "clicks", "type": "INTEGER"},
        {"name": "spend", "type": "FLOAT"},
        {
            "name": "actions",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "FLOAT"},
                {"name": "_1d_click", "type": "FLOAT"},
                {"name": "_7d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
            ],
        },
        {
            "name": "action_values",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "FLOAT"},
                {"name": "_1d_click", "type": "FLOAT"},
                {"name": "_7d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
            ],
        },
        {
            "name": "cost_per_action_type",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "FLOAT"},
                {"name": "_1d_click", "type": "FLOAT"},
                {"name": "_7d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
            ],
        },
        {
            "name": "cost_per_unique_action_type",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "FLOAT"},
                {"name": "_1d_click", "type": "FLOAT"},
                {"name": "_7d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
            ],
        },
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ],
    "keys": {
        "p_key": [
            "date_start",
            "date_stop",
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
        ],
        "incre_key": "_batched_at",
    },
}
