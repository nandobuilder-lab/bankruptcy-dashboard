-- National 12-month rolling filings by chapter
SELECT
    period_end,
    total_all,
    total_ch7,
    total_ch11,
    total_ch13,
    biz_ch11,
    nonbiz_ch11,
    biz_all,
    ROUND(total_ch7  / total_all, 4) AS ch7_pct,
    ROUND(total_ch11 / total_all, 4) AS ch11_pct,
    ROUND(total_ch13 / total_all, 4) AS ch13_pct,
    ROUND(biz_ch11   / biz_all,   4) AS biz_ch11_pct
FROM f2_filings
WHERE row_type = 'national' AND period_months = 12
ORDER BY period_end
