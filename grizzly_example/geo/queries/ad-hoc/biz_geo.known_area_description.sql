-- Column description
ALTER TABLE IF EXISTS `biz_geo.known_area`
ALTER COLUMN IF EXISTS state SET OPTIONS (description="Example column description");

-- Table description
ALTER TABLE IF EXISTS `biz_geo.known_area`
SET OPTIONS (description="Example table description");
