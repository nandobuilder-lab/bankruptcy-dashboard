-- State-level aggregation for latest annual release, with YoY vs prior year
WITH state_expr AS (
    SELECT *,
        CASE
            WHEN REGEXP_REPLACE(TRIM(SPLIT_PART(label, ',', 1)), '[^A-Za-z]', '', 'g') = 'GUAM' THEN 'GU'
            ELSE REGEXP_REPLACE(TRIM(SPLIT_PART(label, ',', 1)), '[^A-Za-z]', '', 'g')
        END AS state
    FROM f2_filings
    WHERE row_type = 'district' AND period_months = 12
),
cur AS (
    SELECT state,
        SUM(total_all)  AS total_all,
        SUM(total_ch7)  AS total_ch7,
        SUM(total_ch11) AS total_ch11,
        SUM(total_ch13) AS total_ch13,
        SUM(biz_ch11)   AS biz_ch11,
        SUM(biz_all)    AS biz_all
    FROM state_expr
    WHERE release_id = (SELECT MAX(release_id) FROM releases)
    GROUP BY state
),
prior AS (
    SELECT state, SUM(total_all) AS total_all
    FROM state_expr
    WHERE release_id = (
        SELECT release_id FROM releases ORDER BY period_end DESC LIMIT 1 OFFSET 4
    )
    GROUP BY state
)
SELECT
    c.state,
    c.total_all,
    c.total_ch7,
    c.total_ch11,
    c.total_ch13,
    c.biz_ch11,
    ROUND(c.total_ch7  / c.total_all, 4) AS ch7_pct,
    ROUND(c.total_ch13 / c.total_all, 4) AS ch13_pct,
    ROUND(c.biz_ch11   / c.total_all, 4) AS biz_ch11_pct,
    ROUND((c.total_all - p.total_all) / p.total_all, 4) AS yoy_pct
FROM cur c
LEFT JOIN prior p ON c.state = p.state
WHERE c.state NOT IN ('GU', 'PR', 'VI', 'NMI')
ORDER BY c.total_all DESC
