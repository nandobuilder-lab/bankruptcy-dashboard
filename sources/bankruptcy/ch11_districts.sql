-- Top 15 districts by business Ch.11 (latest annual)
SELECT
    label AS district,
    biz_ch11,
    total_ch11,
    total_all,
    ROUND(biz_ch11 / NULLIF(total_all, 0), 4) AS biz_ch11_pct
FROM f2_filings
WHERE period_months = 12
  AND row_type = 'district'
  AND release_id = (SELECT MAX(release_id) FROM releases)
ORDER BY biz_ch11 DESC NULLS LAST
LIMIT 15
