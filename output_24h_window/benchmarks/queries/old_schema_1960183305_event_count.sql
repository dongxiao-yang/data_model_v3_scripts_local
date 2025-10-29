SELECT
    deviceCategory, countryIso, platform,
    SUM(metricIntGroup15['event_count']) as metric_sum
FROM default.eco_cross_page_flow_pt1m_local_20251008_3cust
WHERE customerId = 1960183305
  AND flowId = 'session'
  AND metricIntGroup15['event_count'] > 0
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-08 06:00:00'
GROUP BY deviceCategory, countryIso, platform
ORDER BY deviceCategory, countryIso, platform