SELECT
  IF (LENGTH(ap.Postcode)<4, LPAD(ap.Postcode, 4, '0'), ap.Postcode) AS postcode,
  ap.State__Territory1 AS state_erritory,
  ap.Total AS total_population,
  ap.age_65_over AS over_65_population,
  ap.age_65_over / ap.Total AS over_65_population_rate
FROM `bas_geo_australia.tax_individual_age_by_postcode` AS ap