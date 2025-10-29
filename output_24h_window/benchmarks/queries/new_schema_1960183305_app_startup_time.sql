SELECT
    deviceCategory, countryIso, platform,
    SUM(int95) as metric_sum
FROM default.eco_cross_page_preagg_pt1m_3cust
WHERE customerId = 1960183305
  AND int95 > 0
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-08 06:00:00'
GROUP BY deviceCategory, countryIso, platform
ORDER BY deviceCategory, countryIso, platform