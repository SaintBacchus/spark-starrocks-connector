CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    user_city   varchar(64) NOT NULL COMMENT 'user city',
    user_id     BIGINT      NOT NULL COMMENT 'user ID',
    access_time DATETIME    NOT NULL COMMENT 'access time',
    access_cnt  INT         NOT NULL COMMENT 'access count'
) ENGINE = OLAP PRIMARY KEY(user_city, user_id, access_time)
PARTITION BY date_trunc('day', access_time)
DISTRIBUTED BY HASH(user_id)
PROPERTIES (
    "replication_num" = "1"
);