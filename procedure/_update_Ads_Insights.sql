BEGIN CREATE
OR REPLACE TABLE Facebook.Ads_Insights_VuaNemTk01 PARTITION BY date_start AS
SELECT
    *
EXCEPT
(row_num)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY date_start,
                campaign_id,
                adset_id,
                ad_id
                ORDER BY
                    _batched_at
            ) AS row_num
        FROM
            Facebook._stage_Ads_Insights_VuaNemTk01
    )
WHERE
    row_num = 1;

CREATE
OR REPLACE TABLE Facebook.Ads_Insights_VuaNemUSD PARTITION BY date_start AS
SELECT
    *
EXCEPT
(row_num)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY date_start,
                campaign_id,
                adset_id,
                ad_id
                ORDER BY
                    _batched_at
            ) AS row_num
        FROM
            Facebook._stage_Ads_Insights_VuaNemUSD
    )
WHERE
    row_num = 1;

END
