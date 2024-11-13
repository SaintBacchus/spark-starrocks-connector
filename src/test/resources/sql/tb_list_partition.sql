CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_id     BIGINT      NOT NULL COMMENT 'user ID',
    city        VARCHAR(32) NOT NULL COMMENT 'city name',
    shop_id     BIGINT      NOT NULL COMMENT 'SHOP ID',
    access_date DATE        NOT NULL COMMENT 'access date'
) ENGINE = OLAP DUPLICATE KEY(user_id)
PARTITION BY LIST (shop_id, city) (
    PARTITION p1 VALUES IN (('101', 'shanghai'), ('101', 'zhejiang'), ('101', 'jiangsu')),
    PARTITION p2 VALUES IN (('102', 'shanghai'), ('102', 'zhejiang'), ('102', 'jiangsu')),
    PARTITION p3 VALUES IN (('103', 'shanghai'), ('103', 'zhejiang'), ('103', 'jiangsu')),
    PARTITION p4 VALUES IN (('104', 'shanghai'), ('104', 'zhejiang'), ('104', 'jiangsu'))
)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);