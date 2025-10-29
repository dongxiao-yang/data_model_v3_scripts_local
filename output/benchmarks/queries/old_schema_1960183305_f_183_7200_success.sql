SELECT
    deviceCategory, countryIso, platform,
    SUM(metricIntGroup14['f_183_7200_success']) as metric_sum
FROM default.eco_cross_page_flow_pt1m_local_20251008_3cust
WHERE customerId = 1960183305
  AND flowId = 'f_183_7200'
  AND metricIntGroup14['f_183_7200_success'] > 0
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-09 00:00:00'
GROUP BY deviceCategory, countryIso, platform
ORDER BY deviceCategory, countryIso, platform