SELECT
    deviceCategory, countryIso, platform,
    SUM(int26) as metric_sum
FROM default.eco_cross_page_preagg_pt1m_4cust
WHERE customerId = 1960181845
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-09 00:00:00'
GROUP BY deviceCategory, countryIso, platform