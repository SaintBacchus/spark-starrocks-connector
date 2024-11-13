CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_id BIGINT   NOT NULL COMMENT 'user ID',
    site_id CHAR(32) NOT NULL COMMENT 'site ID',
    pv      BIGINT REPLACE,
    pv_sum  BIGINT SUM DEFAULT "0",
    pv_min  BIGINT MIN,
    pv_max  BIGINT MAX
) ENGINE = OLAP AGGREGATE KEY(user_id, site_id)
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
    "replication_num" = "1"
);