-- Monthly national filings (last 36 months)
SELECT period_end, total_all, total_ch7, total_ch11, total_ch13
FROM f2_filings
WHERE row_type = 'national' AND period_months = 1
ORDER BY period_end DESC
LIMIT 36
