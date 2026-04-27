-- 3-month snapshots with sequential QoQ and same-quarter prior-year (SQPY) growth
WITH q AS (
    SELECT
        period_end,
        total_all,
        total_ch11,
        EXTRACT(month FROM period_end) AS qm,
        LAG(total_all)  OVER (ORDER BY period_end) AS prev_all,
        LAG(total_ch11) OVER (ORDER BY period_end) AS prev_ch11
    FROM f2_filings
    WHERE row_type = 'national' AND period_months = 3
)
SELECT
    a.period_end,
    a.total_all,
    ROUND((a.total_all  - a.prev_all)  / a.prev_all,  4) AS qoq_pct,
    ROUND((a.total_all  - b.total_all) / b.total_all, 4) AS sqpy_pct,
    a.total_ch11,
    ROUND((a.total_ch11 - a.prev_ch11)  / a.prev_ch11,  4) AS ch11_qoq_pct,
    ROUND((a.total_ch11 - b.total_ch11) / b.total_ch11, 4) AS ch11_sqpy_pct
FROM q a
LEFT JOIN q b ON b.qm = a.qm AND b.period_end = a.period_end - INTERVAL '1 year'
WHERE a.period_end >= '2022-06-30'
ORDER BY a.period_end DESC
