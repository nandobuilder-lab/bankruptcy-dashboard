-- KPI snapshot: latest annual national figures + YoY
WITH annual AS (
    SELECT period_end, total_all, total_ch7, total_ch11, total_ch13, biz_ch11
    FROM f2_filings
    WHERE row_type = 'national' AND period_months = 12
    ORDER BY period_end
)
SELECT
    a.period_end,
    a.total_all,
    a.total_ch7,
    a.total_ch11,
    a.total_ch13,
    a.biz_ch11,
    ROUND((a.total_all - b.total_all) * 100.0 / b.total_all, 1) AS yoy_pct,
    ROUND((a.total_ch11 - b.total_ch11) * 100.0 / b.total_ch11, 1) AS ch11_yoy_pct
FROM annual a
JOIN annual b ON b.period_end = a.period_end - INTERVAL '1 year'
ORDER BY a.period_end DESC
LIMIT 1
