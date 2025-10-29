SELECT
    deviceCategory, countryIso, platform,
    SUM(metricIntGroup15['f_202_363_total_duration']) as metric_sum
FROM default.eco_cross_page_flow_pt1m_local_20251008_3cust
WHERE customerId = 1960181845
  AND flowId = 'f_202_363'
  AND metricIntGroup15['f_202_363_total_duration'] > 0
  AND timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-09 00:00:00'
GROUP BY deviceCategory, countryIso, platform
ORDER BY deviceCategory, countryIso, platform