CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_id BIGINT      NOT NULL COMMENT 'user ID',
    site_id VARCHAR(32) NOT NULL COMMENT 'site ID',
    pv      BIGINT      NOT NULL DEFAULT "0" COMMENT 'total page views'
) ENGINE = OLAP PRIMARY KEY (user_id, site_id)
DISTRIBUTED BY HASH(site_id)
ORDER BY(user_id)
PROPERTIES (
    "replication_num" = "1"
);