SELECT
    deviceCategory, countryIso, platform,
    SUM(`f_202_363_total_duration`) as metric_sum
FROM eco_cross_page_customer_1960181845_view
WHERE timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-09 00:00:00'
GROUP BY deviceCategory, countryIso, platform