from facebook.pipeline.interface import AdsInsights

platform_position_insights = AdsInsights(
    "PlatformPositionInsights",
    "account",
    [
        "date_start",
        "date_stop",
        "account_id",
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
    lambda rows: [
        {
            "account_id": row["account_id"],
            "date_start": row["date_start"],
            "date_stop": row["date_stop"],
            "publisher_platform": row["publisher_platform"],
            "platform_position": row["platform_position"],
            "impressions": row.get("impressions"),
            "cpc": row.get("cpc"),
            "cpm": row.get("cpm"),
            "ctr": row.get("ctr"),
            "clicks": row.get("clicks"),
            "spend": row["spend"],
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
    [
        {"name": "account_id", "type": "NUMERIC"},
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
        {"name": "publisher_platform", "type": "STRING"},
        {"name": "platform_position", "type": "STRING"},
        {"name": "reach", "type": "NUMERIC"},
        {"name": "impressions", "type": "NUMERIC"},
        {"name": "cpc", "type": "NUMERIC"},
        {"name": "cpm", "type": "NUMERIC"},
        {"name": "ctr", "type": "NUMERIC"},
        {"name": "clicks", "type": "NUMERIC"},
        {"name": "spend", "type": "NUMERIC"},
        {
            "name": "actions",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
            ],
        },
        {
            "name": "action_values",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
            ],
        },
        {
            "name": "cost_per_action_type",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
            ],
        },
        {
            "name": "cost_per_unique_action_type",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "action_type", "type": "STRING"},
                {"name": "value", "type": "NUMERIC"},
                {"name": "_1d_click", "type": "NUMERIC"},
                {"name": "_1d_view", "type": "NUMERIC"},
                {"name": "_7d_view", "type": "NUMERIC"},
                {"name": "_7d_click", "type": "NUMERIC"},
            ],
        },
    ],
    [
        "date_start",
        "account_id",
        "date_stop",
        "publisher_platform",
        "platform_position",
    ],
    "publisher_platform,platform_position",
)
