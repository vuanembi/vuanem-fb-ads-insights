from models.models import FacebookAdsInsights


class AdsInsights_Region(FacebookAdsInsights):
    keys = {
        "p_key": [
            "date_start",
            "date_stop",
            "region",
        ],
        "incre_key": "_batched_at",
    }

    fields = [
        "date_start",
        "date_stop",
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
    ]

    level = "account"
    breakdowns = "region"

    schema = [
        {"name": "account_id", "type": "INTEGER"},
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
        {"name": "region", "type": "STRING"},
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
                {"name": "_28d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
                {"name": "_28d_view", "type": "FLOAT"},
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
                {"name": "_28d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
                {"name": "_28d_view", "type": "FLOAT"},
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
                {"name": "_28d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
                {"name": "_28d_view", "type": "FLOAT"},
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
                {"name": "_28d_click", "type": "FLOAT"},
                {"name": "_1d_view", "type": "FLOAT"},
                {"name": "_7d_view", "type": "FLOAT"},
                {"name": "_28d_view", "type": "FLOAT"},
            ],
        },
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]

    def transform(self, rows):
        return [
            {
                "account_id": row["account_id"],
                "date_start": row["date_start"],
                "date_stop": row["date_stop"],
                "region": row["region"],
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
                        "_28d_view": action.get("28d_view"),
                        "_28d_click": action.get("28d_click"),
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
                        "_28d_view": action.get("28d_view"),
                        "_28d_click": action.get("28d_click"),
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
                        "_28d_view": action.get("28d_view"),
                        "_28d_click": action.get("28d_click"),
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
                        "_28d_view": action.get("28d_view"),
                        "_28d_click": action.get("28d_click"),
                    }
                    for action in row["cost_per_unique_action_type"]
                ]
                if row.get("cost_per_unique_action_type", [])
                else [],
            }
            for row in rows
        ]
