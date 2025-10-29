SELECT
    toStartOfMinute(timestampMs) as minute,
    SUM(int97) as metric_sum
FROM default.eco_cross_page_preagg_pt1m_3cust
WHERE customerId = 1960183305
  AND int97 > 0
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-09 00:00:00'
GROUP BY minute
ORDER BY minute