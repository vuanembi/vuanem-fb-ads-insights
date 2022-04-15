from typing import Optional, Union, Any
from datetime import datetime, timedelta

from compose import compose

from facebook.pipeline.interface import AdsInsights
from facebook.facebook_repo import get
from db.bigquery import load

DATE_FORMAT = "%Y-%m-%d"


def _timeframe_service(
    start: Optional[str],
    end: Optional[str],
) -> tuple[datetime, datetime]:
    return (
        (datetime.utcnow() - timedelta(days=8))
        if not start
        else datetime.strptime(start, DATE_FORMAT)
    ), datetime.utcnow() if not end else datetime.strptime(end, DATE_FORMAT)


def _batched_at_service(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "_batched_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        for row in rows
    ]


def pipeline_service(pipeline: AdsInsights):
    def run(
        ads_account_id: str,
        start: Optional[str],
        end: Optional[str],
    ) -> dict[str, Union[str, int]]:
        return compose(
            lambda x: {
                "table": pipeline.name,
                "ads_account_id": ads_account_id,
                "start": start,
                "end": end,
                "output_rows": x,
            },
            load(pipeline.name, pipeline.schema, pipeline.id_key, ads_account_id),
            _batched_at_service,
            pipeline.transform,
            get(pipeline.level, pipeline.fields, pipeline.breakdowns),
        )(
            ads_account_id,
            *_timeframe_service(start, end),
        )

    return run
