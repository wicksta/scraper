-- MySQL migration: cache a correspondence postal address for each LPA.
-- Target DB: wickhams_monitor (or MYSQL_DATABASE in runtime env)
--
-- Rollback:
--   ALTER TABLE `lpa_codes` DROP COLUMN `postal_address`;

SET @has_postal_address := (
  SELECT COUNT(*)
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'lpa_codes'
    AND COLUMN_NAME = 'postal_address'
);

SET @ddl := IF(
  @has_postal_address = 0,
  'ALTER TABLE `lpa_codes` ADD COLUMN `postal_address` VARCHAR(500) NULL AFTER `website`',
  'SELECT 1'
);

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Initial verified value supplied for the Westminster planning authority.
UPDATE `lpa_codes`
SET `postal_address` = 'Westminster City Council, Westminster City Hall, 64 Victoria Street, London, SW1E 6QP'
WHERE `ons_code` = 'E09000033';
