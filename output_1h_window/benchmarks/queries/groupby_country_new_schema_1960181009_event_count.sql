SELECT
    country,
    SUM(int22) as metric_sum
FROM default.rui_eco_cross_page_preagg_pt1m_1h
WHERE customerId = 1960181009
  AND int22 > 0
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-08 06:00:00'
GROUP BY country
ORDER BY country