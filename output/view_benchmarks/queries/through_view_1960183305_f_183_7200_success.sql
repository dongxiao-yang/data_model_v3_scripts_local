SELECT
    deviceCategory, countryIso, platform,
    SUM(`f_183_7200_success`) as metric_sum
FROM eco_cross_page_customer_1960183305_view
WHERE timestampMs >= '2025-10-08 00:00:00'
  AND timestampMs < '2025-10-09 00:00:00'
GROUP BY deviceCategory, countryIso, platform