CREATE TABLE IF NOT EXISTS `%s`.`%s`
(
    id                      BIGINT,
    binary_null_value       BINARY                                                              REPLACE,
    binary_value            BINARY                                                              REPLACE,
    varbinary_null_value    VARBINARY(128)                                                      REPLACE,
    varbinary_value         VARBINARY(128)                                                      REPLACE,
    bitmap_null_value       BITMAP                                                              BITMAP_UNION,
    bitmap_value            BITMAP                                                              BITMAP_UNION,
    hll_null_value          HLL                                                                 HLL_UNION,
    hll_value               HLL                                                                 HLL_UNION,
    array_null_value        ARRAY<INT>                                                          REPLACE,
    array_int_value         ARRAY<INT>                                                          REPLACE,
    array_array_int_value   ARRAY<ARRAY<INT>>                                                   REPLACE,
    struct_null_value       STRUCT<name STRING, age INT>                                        REPLACE,
    struct_value            STRUCT<name STRING, age INT>                                        REPLACE,
    struct_struct_value     STRUCT<name STRING, addr STRUCT<prov STRING, city STRING>>          REPLACE,
    map_null_value          MAP<STRING, INT>                                                    REPLACE,
    map_value               MAP<STRING, INT>                                                    REPLACE,
    map_map_value           MAP<STRING, MAP<STRING, INT>>                                       REPLACE,
    nested_null_value       MAP<BIGINT, STRUCT<name STRING, age INT, hobbies ARRAY<STRING>>>    REPLACE,
    nested_value            MAP<BIGINT, STRUCT<name STRING, age INT, hobbies ARRAY<STRING>>>    REPLACE
) ENGINE = OLAP AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);