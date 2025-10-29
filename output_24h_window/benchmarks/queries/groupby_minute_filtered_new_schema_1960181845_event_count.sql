SELECT
    toStartOfMinute(timestampMs) as minute,
    SUM(int18) as metric_sum
FROM default.eco_cross_page_preagg_pt1m_3cust
WHERE customerId = 1960181845
  AND int18 > 0
  AND timestampMs >= toDateTime('2025-10-08 00:00:00')
  AND timestampMs < toDateTime('2025-10-08 06:00:00')
GROUP BY minute
ORDER BY minute