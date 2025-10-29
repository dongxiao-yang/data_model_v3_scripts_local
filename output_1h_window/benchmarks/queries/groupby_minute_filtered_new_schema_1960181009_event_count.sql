SELECT
    toStartOfMinute(timestampMs) as minute,
    SUM(int22) as metric_sum
FROM default.rui_eco_cross_page_preagg_pt1m_1h
WHERE customerId = 1960181009
  AND int22 > 0
  AND timestampMs >= toDateTime('2025-10-08 00:00:00')
  AND timestampMs < toDateTime('2025-10-08 06:00:00')
GROUP BY minute
ORDER BY minute