CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    act_time BIGINT         NOT NULL COMMENT 'action timestamp',
    user_id  BIGINT         NOT NULL COMMENT 'user ID',
    act_type VARCHAR(128)   NOT NULL COMMENT 'action type',
    status   VARCHAR(32)    NOT NULL COMMENT 'action status',
    `desc`   VARCHAR(65535) NULL COMMENT 'description'
) ENGINE = OLAP DUPLICATE KEY(act_time, user_id)
DISTRIBUTED BY HASH(user_id)
PROPERTIES (
    "replication_num" = "1"
);