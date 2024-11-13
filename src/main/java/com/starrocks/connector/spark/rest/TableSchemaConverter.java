// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.rest;

import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlColumn;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlIndex;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlPartition;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlPartitionInfo;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlTable;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.format.rest.model.Column;
import com.starrocks.format.rest.model.DistributionInfo;
import com.starrocks.format.rest.model.MaterializedIndexMeta;
import com.starrocks.format.rest.model.PartitionInfo;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TablePartition.Tablet;
import com.starrocks.format.rest.model.TableSchema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class TableSchemaConverter implements BiFunction<TableSchema, List<TablePartition>, StarRocksSchema> {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String SHADOW_AUTOMATIC_PARTITION_NAME = "$shadow_automatic_partition";

    @Override
    public StarRocksSchema apply(TableSchema tableSchema, List<TablePartition> tablePartitions) {
        List<StarRocksField> fields = new ArrayList<>();
        List<StarRocksField> keyFields = new ArrayList<>();

        List<Column> columns = tableSchema.getColumns();
        if (CollectionUtils.isNotEmpty(columns)) {
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                StarRocksField field = new StarRocksField(
                        column.getName(),
                        Optional.ofNullable(column.getType())
                                // FIXME backwards compatibility
                                .orElseGet(() -> {
                                    Column.Type colType = new Column.Type();
                                    colType.setName(column.getPrimitiveType());
                                    colType.setTypeSize(column.getPrimitiveTypeSize());
                                    colType.setColumnSize(column.getColumnSize());
                                    colType.setPrecision(column.getPrecision());
                                    colType.setScale(column.getScale());
                                    return colType;
                                }),
                        i + 1,
                        Boolean.TRUE.equals(column.getAllowNull())
                );
                fields.add(field);
                if (Boolean.TRUE.equals(column.getKey())) {
                    keyFields.add(field);
                }
            }
        }

        // indexes
        List<EtlIndex> etlIndexes = new ArrayList<>();
        for (MaterializedIndexMeta indexMeta : tableSchema.getIndexMetas()) {
            List<EtlColumn> etlColumns = new ArrayList<>();
            for (int i = 0; i < indexMeta.getColumns().size(); i++) {
                Column column = indexMeta.getColumns().get(i);
                if (null == column.getUniqueId()) {
                    column.setUniqueId(i);
                }
                etlColumns.add(toEtlColumn(column));
            }
            etlIndexes.add(
                    new EtlIndex(
                            indexMeta.getIndexId(),
                            etlColumns,
                            indexMeta.getKeysType(),
                            tableSchema.getBaseIndexId().equals(indexMeta.getIndexId())
                    )
            );
        }

        // partition info
        PartitionInfo partitionInfo = tableSchema.getPartitionInfo();
        List<String> partitionColumnRefs = Optional.ofNullable(partitionInfo.getPartitionColumns())
                .map(cols -> cols.stream().map(Column::getName).collect(Collectors.toList()))
                .orElse(new ArrayList<>(0));

        DistributionInfo distributionInfo = tableSchema.getDefaultDistributionInfo();
        List<String> distributionColumnRefs = Optional.ofNullable(distributionInfo.getDistributionColumns())
                .map(cols -> cols.stream().map(Column::getName).collect(Collectors.toList()))
                .orElse(new ArrayList<>(0));

        List<EtlPartition> etlPartitions = Optional.ofNullable(tablePartitions)
                .map(partitions -> partitions.stream()
                        .filter(tp -> !SHADOW_AUTOMATIC_PARTITION_NAME.equals(tp.getName()))
                        .map(TableSchemaConverter::toEtlPartition)
                        .collect(Collectors.toList()))
                .orElse(new ArrayList<>(0));

        EtlPartitionInfo etlPartitionInfo = new EtlPartitionInfo(
                partitionInfo.getType(),
                partitionColumnRefs,
                distributionColumnRefs,
                etlPartitions
        );

        EtlTable etlTable = new EtlTable(etlIndexes, etlPartitionInfo);
        return new StarRocksSchema(
                fields,
                keyFields,
                etlTable,
                tableSchema.getId()
        );
    }

    private static EtlColumn toEtlColumn(Column column) {
        String columnName = column.getName();

        // default value
        String defaultValue = column.getDefaultValue();
        String defaultValueType = column.getDefaultValueType();
        if ("VARY".equalsIgnoreCase(defaultValueType)) {
            throw new StarRocksException(
                    "Column " + columnName + " has unsupported default value:" + column.getDefaultExpr());
        }
        if ("CONST".equalsIgnoreCase(defaultValueType)) {
            defaultValue = calculatedDefaultValue(column);
        }

        boolean allowNull = Boolean.TRUE.equals(column.getAllowNull());
        if (allowNull && "NULL".equalsIgnoreCase(defaultValueType)) {
            defaultValue = "\\N";
        }

        // string length
        return new EtlColumn(
                columnName,
                Optional.ofNullable(column.getType())
                        // FIXME backwards compatibility
                        .orElseGet(() -> {
                            Column.Type colType = new Column.Type();
                            colType.setName(column.getPrimitiveType());
                            colType.setTypeSize(column.getPrimitiveTypeSize());
                            colType.setColumnSize(column.getColumnSize());
                            colType.setPrecision(column.getPrecision());
                            colType.setScale(column.getScale());
                            return colType;
                        }),
                allowNull,
                Boolean.TRUE.equals(column.getAutoIncrement()),
                Boolean.TRUE.equals(column.getKey()),
                column.getAggregationType(),
                defaultValue,
                column.getUniqueId()
        );
    }

    private static String calculatedDefaultValue(Column column) {
        String defaultValue = column.getDefaultValue();
        if (defaultValue != null) {
            return defaultValue;
        }

        String defaultExpr = column.getDefaultExpr();
        if ("now()".equalsIgnoreCase(defaultExpr)) {
            // should not run up here
            return LocalDateTime.now().format(DATE_TIME_FORMATTER);
        }
        return null;
    }

    private static int columnLength(Column.Type colType) {
        switch (StringUtils.upperCase(colType.getName())) {
            case "BOOLEAN":
            case "DATE":
            case "DATETIME":
                return colType.getTypeSize();
            default:
                return colType.getColumnSize() == null ? 0 : colType.getColumnSize();
        }
    }

    private static EtlPartition toEtlPartition(TablePartition partition) {
        List<Long> tabletIds = new ArrayList<>();
        List<Long> backendIds = new ArrayList<>();
        List<String> metaUrls = new ArrayList<>();
        List<Tablet> tablets = partition.getTablets();
        if (CollectionUtils.isNotEmpty(tablets)) {
            tabletIds.addAll(
                    tablets.stream()
                            .map(Tablet::getId).collect(Collectors.toList())
            );
            backendIds.addAll(
                    tablets.stream()
                            .map(tabletPartition -> {
                                        if (tabletPartition.getPrimaryComputeNodeId() != null) {
                                            return tabletPartition.getPrimaryComputeNodeId();
                                        } else {
                                            return tabletPartition.getBackendIds().stream().findAny().get();
                                        }
                                    }
                            ).collect(Collectors.toList())
            );

            // TODO:: Seem only one replica can be used.
            metaUrls.addAll(tablets.stream()
                    .map(p -> p.getMetaUrls().stream().findFirst().orElse("")).collect(Collectors.toList())
            );
        }

        return new EtlPartition(
                partition.getId(),
                partition.getStartKeys(),
                partition.getEndKeys(),
                partition.getInKeys(),
                Boolean.TRUE.equals(partition.getMinPartition()),
                Boolean.TRUE.equals(partition.getMaxPartition()),
                partition.getBucketNum(),
                partition.getStoragePath(),
                tabletIds,
                backendIds,
                metaUrls
        );
    }

}
