from typing import Any

from google.cloud import bigquery

client = bigquery.Client()

DATASET = "IP_Facebook"


def _batched_at_schema(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return schema + [{"name": "_batched_at", "type": "TIMESTAMP"}]


def load(
    table: str,
    schema: list[dict[str, Any]],
    id_key: list[str],
    ads_account_id: str,
):
    def _load(data: list[dict[str, Any]]) -> int:
        if len(data) == 0:
            return 0

        output_rows = (
            client.load_table_from_json(
                data,
                f"{DATASET}.{table}_{ads_account_id}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=_batched_at_schema(schema),
                ),
            )
            .result()
            .output_rows  # type: ignore
        )
        _update(table, id_key, ads_account_id)
        return output_rows

    return _load


def _update(table: str, id_key: list[str], ads_account_id: str):
    client.query(
        f"""
    CREATE OR REPLACE TABLE {DATASET}.{table}_{ads_account_id} AS
    SELECT * EXCEPT(row_num) FROM
    (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY {','.join(id_key)}
            ORDER BY _batched_at DESC) AS row_num
        FROM {DATASET}.{table}_{ads_account_id}
    ) WHERE row_num = 1"""
    ).result()
