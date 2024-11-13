package com.starrocks.connector.spark.sql;


public enum Table {

    TB_DUPLICATE_KEY("tb_duplicate_key"),
    TB_AGGREGATE_KEY("tb_aggregate_key"),
    TB_UNIQUE_KEY("tb_unique_key"),
    TB_PRIMARY_KEY("tb_primary_key"),

    TB_DUPLICATE_TYPES("tb_duplicate_types"),
    TB_AGGREGATE_TYPES("tb_aggregate_types"),
    TB_UNIQUE_TYPES("tb_unique_types"),
    TB_PRIMARY_TYPES("tb_primary_types"),

    TB_SPECIFIC_DATA_TYPES_SOURCE("tb_specific_data_types"),
    TB_SPECIFIC_DATA_TYPES_TARGET("tb_specific_data_types"),

    TB_RANGE_PARTITION("tb_range_partition"),
    TB_LIST_PARTITION("tb_list_partition"),
    TB_EXPR_PARTITION("tb_expr_partition"),

    TB_FILTER_PUSHDOWN("tb_filter_pushdown"),
    TB_TRANSACTION("tb_transaction"),

    TB_BATCH_INSERT("tb_batch_insert");

    private final String sqlTemplateName;

    Table(String sqlTemplateName) {
        this.sqlTemplateName = sqlTemplateName;
    }

    public String getTableName() {
        return name().toLowerCase();
    }

    public String getSqlTemplateName() {
        return sqlTemplateName;
    }

}
