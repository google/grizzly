SELECT
  FORMAT('%04d', m.Postcode) AS postcode,
  m.Count_taxable_income_or_loss AS count_taxable_income_or_loss,
  m.Average_total_income_or_loss AS average_total_income_or_loss,
  m.Median_total_income_or_loss AS median_total_income_or_loss
FROM `bas_geo_australia.tax_income_average_median_by_postcode` AS m