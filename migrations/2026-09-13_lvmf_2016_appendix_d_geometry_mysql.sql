-- MySQL migration: complete Appendix D LVMF geometry, separate from the
-- compatibility-only fixed-column `lvmf` table.
CREATE TABLE IF NOT EXISTS `lvmf_2016_appendix_d_datasets` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `source_name` VARCHAR(255) NOT NULL,
  `source_sha256` CHAR(64) NOT NULL,
  `source_path` VARCHAR(500) NOT NULL,
  `imported_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `lvmf_2016_appendix_d_datasets_source_uq` (`source_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `lvmf_2016_appendix_d_areas` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `dataset_id` BIGINT UNSIGNED NOT NULL,
  `source_row_number` INT NOT NULL,
  `view_ref` VARCHAR(32) NOT NULL,
  `assessment_from` TEXT NOT NULL,
  `assessment_to` VARCHAR(255) NOT NULL,
  `area_code` VARCHAR(64) NOT NULL,
  `area_name` VARCHAR(255) NOT NULL,
  `geometry_class` VARCHAR(128) NOT NULL,
  `coordinate_system` VARCHAR(255) NOT NULL,
  `vertex_count` SMALLINT NOT NULL,
  `geometry_wkt_3d` LONGTEXT NOT NULL,
  `length_m` DECIMAL(12,3) NULL,
  `length_reference` VARCHAR(32) NULL,
  `width_at_monument_m` DECIMAL(12,3) NULL,
  `width_reference` VARCHAR(32) NULL,
  `source_pdf_page` INT NULL,
  `source_printed_page` INT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `lvmf_2016_appendix_d_areas_source_row_uq` (`dataset_id`,`source_row_number`),
  KEY `lvmf_2016_appendix_d_areas_view_idx` (`dataset_id`,`view_ref`,`area_code`),
  CONSTRAINT `lvmf_2016_appendix_d_areas_dataset_fk` FOREIGN KEY (`dataset_id`) REFERENCES `lvmf_2016_appendix_d_datasets` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `lvmf_2016_appendix_d_area_vertices` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `area_id` BIGINT UNSIGNED NOT NULL,
  `vertex_order` SMALLINT NOT NULL,
  `point_label` VARCHAR(16) NOT NULL,
  `easting` DECIMAL(12,3) NOT NULL,
  `northing` DECIMAL(12,3) NOT NULL,
  `height_m_aod` DECIMAL(8,3) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `lvmf_2016_appendix_d_vertices_uq` (`area_id`,`vertex_order`),
  KEY `lvmf_2016_appendix_d_vertices_label_idx` (`point_label`),
  CONSTRAINT `lvmf_2016_appendix_d_vertices_area_fk` FOREIGN KEY (`area_id`) REFERENCES `lvmf_2016_appendix_d_areas` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Rollback:
-- DROP TABLE `lvmf_2016_appendix_d_area_vertices`;
-- DROP TABLE `lvmf_2016_appendix_d_areas`;
-- DROP TABLE `lvmf_2016_appendix_d_datasets`;
