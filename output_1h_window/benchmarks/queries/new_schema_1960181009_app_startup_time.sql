SELECT
    deviceCategory, countryIso, platform,
    SUM(int21) as metric_sum
FROM default.rui_eco_cross_page_preagg_pt1m_1h
WHERE customerId = 1960181009
  AND int21 > 0
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-08 06:00:00'
GROUP BY deviceCategory, countryIso, platform
ORDER BY deviceCategory, countryIso, platform