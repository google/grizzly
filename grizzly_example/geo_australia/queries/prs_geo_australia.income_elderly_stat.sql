SELECT
  pc.POA_CODE21 AS postcode,
  e.state_erritory,
  e.total_population,
  e.over_65_population,
  e.over_65_population_rate,
  inc.median_total_income_or_loss,
  inc.median_total_income_or_loss * e.over_65_population_rate AS income_metric,
  pc.AREASQKM21 AS area_sqkm,
  ST_GEOGFROMTEXT(pc.geometry) AS geometry
FROM `bas_geo_australia.post_codes` AS pc
LEFT OUTER JOIN `biz_geo_australia.tax_elderly_by_postcode` AS e
  ON pc.POA_CODE21 = e.postcode
LEFT OUTER JOIN `biz_geo_australia.tax_income_average_median_by_postcode` AS inc
  ON pc.POA_CODE21 = inc.postcode
WHERE pc.geometry IS NOT NULL