

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sync_checked_keys
-- ----------------------------
DROP TABLE IF EXISTS `sync_checked_keys`;
CREATE TABLE `sync_checked_keys` (
  `check_id` int(11) NOT NULL AUTO_INCREMENT,
  `check_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `table_name` varchar(255) DEFAULT NULL,
  `action` varchar(40) DEFAULT NULL,
  `benchmark` int(11) DEFAULT NULL,
  `data_source` int(11) DEFAULT NULL,
  `duty` text,
  `fee_level` int(11) DEFAULT NULL,
  `fee_type_code` text,
  `fund_id` text,
  `id` text,
  `index_id` text,
  `index_method` int(11) DEFAULT NULL,
  `index_range` int(11) DEFAULT NULL,
  `name` text,
  `org_id` text,
  `org_type_code` int(11) DEFAULT NULL,
  `portfolio_type` int(11) DEFAULT NULL,
  `security_category` text,
  `source_code` int(11) DEFAULT NULL,
  `statistic_date` date DEFAULT NULL,
  `statistic_date_std` date DEFAULT NULL,
  `subfund_id` text,
  `typestandard_code` int(11) DEFAULT NULL,
  `user_id` text,
  PRIMARY KEY (`check_id`)
) ENGINE=InnoDB AUTO_INCREMENT=873375 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for sync_deleted_keys
-- ----------------------------
DROP TABLE IF EXISTS `sync_deleted_keys`;
CREATE TABLE `sync_deleted_keys` (
  `benchmark` int(11) DEFAULT NULL,
  `data_source` int(11) DEFAULT NULL,
  `duty` text,
  `fee_level` int(11) DEFAULT NULL,
  `fee_type_code` text,
  `fund_id` text,
  `id` text,
  `index_id` text,
  `index_method` int(11) DEFAULT NULL,
  `index_range` int(11) DEFAULT NULL,
  `name` text,
  `org_id` text,
  `org_type_code` int(11) DEFAULT NULL,
  `portfolio_type` int(11) DEFAULT NULL,
  `security_category` text,
  `source_code` int(11) DEFAULT NULL,
  `statistic_date` date DEFAULT NULL,
  `statistic_date_std` date DEFAULT NULL,
  `subfund_id` text,
  `typestandard_code` int(11) DEFAULT NULL,
  `user_id` text,
  `table_name` varchar(255) DEFAULT NULL,
  `delete_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `delete_id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`delete_id`)
) ENGINE=InnoDB AUTO_INCREMENT=200390 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for sync_log
-- ----------------------------
DROP TABLE IF EXISTS `sync_log`;
CREATE TABLE `sync_log` (
  `table_name` varchar(255) DEFAULT NULL,
  `log_info` text,
  `status` int(2) DEFAULT NULL,
  `log_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `level` varchar(255) DEFAULT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=31257 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for sync_status
-- ----------------------------
DROP TABLE IF EXISTS `sync_status`;
CREATE TABLE `sync_status` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `latest_update_time` datetime DEFAULT NULL,
  `table_name` varchar(255) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `updated` int(255) DEFAULT NULL,
  `error_info` text,
  `deleted` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=156 DEFAULT CHARSET=utf8;
