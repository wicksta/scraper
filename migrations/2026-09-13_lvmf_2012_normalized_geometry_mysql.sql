-- MySQL migration: normalised, source-traceable 2012 LVMF corridor geometry.
-- This deliberately does not alter the fixed-column `lvmf` table used by the
-- existing legacy viewer.
CREATE TABLE IF NOT EXISTS `lvmf_2012_geometry_datasets` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `source_name` VARCHAR(255) NOT NULL,
  `source_sha256` CHAR(64) NOT NULL,
  `source_path` VARCHAR(500) NOT NULL,
  `imported_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `lvmf_2012_geometry_datasets_source_uq` (`source_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `lvmf_2012_corridors` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `dataset_id` BIGINT UNSIGNED NOT NULL,
  `view_ref` VARCHAR(32) NOT NULL,
  `corridor_code` VARCHAR(32) NOT NULL,
  `from_location` TEXT NOT NULL,
  `to_landmark` VARCHAR(255) NOT NULL,
  `corridor_length_m` DECIMAL(12,3) NULL,
  `width_at_landmark_m` DECIMAL(12,3) NULL,
  `source_pdf_page` INT NULL,
  `calculation_model` VARCHAR(64) NOT NULL,
  `calculation_definition` JSON NOT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `lvmf_2012_corridors_dataset_view_code_uq` (`dataset_id`,`view_ref`,`corridor_code`),
  KEY `lvmf_2012_corridors_view_ref_idx` (`view_ref`),
  CONSTRAINT `lvmf_2012_corridors_dataset_fk` FOREIGN KEY (`dataset_id`) REFERENCES `lvmf_2012_geometry_datasets` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `lvmf_2012_corridor_control_points` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `corridor_id` BIGINT UNSIGNED NOT NULL,
  `point_label` VARCHAR(16) NOT NULL,
  `point_role` VARCHAR(128) NOT NULL,
  `polygon_order` SMALLINT NULL,
  `easting` DECIMAL(12,3) NOT NULL,
  `northing` DECIMAL(12,3) NOT NULL,
  `height_m_aod` DECIMAL(8,3) NOT NULL,
  `source_pdf_page` INT NULL,
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `lvmf_2012_corridor_points_uq` (`corridor_id`,`point_label`),
  KEY `lvmf_2012_corridor_points_polygon_idx` (`corridor_id`,`polygon_order`),
  CONSTRAINT `lvmf_2012_corridor_points_corridor_fk` FOREIGN KEY (`corridor_id`) REFERENCES `lvmf_2012_corridors` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Rollback:
-- DROP TABLE `lvmf_2012_corridor_control_points`;
-- DROP TABLE `lvmf_2012_corridors`;
-- DROP TABLE `lvmf_2012_geometry_datasets`;
