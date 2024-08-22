CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    act_time   BIGINT         NOT NULL COMMENT 'action timestamp',
    user_id    BIGINT         NOT NULL COMMENT 'user ID',
    act_type   VARCHAR(128)   NOT NULL COMMENT 'action type',
    status     VARCHAR(32)    NOT NULL COMMENT 'action status'
) ENGINE = OLAP DUPLICATE KEY(act_time, user_id)
PARTITION BY RANGE (act_time) (
    PARTITION p1 VALUES LESS THAN ("1704038400"),
    PARTITION p2 VALUES LESS THAN ("1706716800"),
    PARTITION p3 VALUES LESS THAN ("1709222400"),
    PARTITION p4 VALUES LESS THAN ("1711900800"),
    PARTITION p5 VALUES LESS THAN ("1714492800")
)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);