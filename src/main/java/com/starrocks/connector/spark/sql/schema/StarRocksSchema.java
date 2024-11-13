// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
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

package com.starrocks.connector.spark.sql.schema;

import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.spark.sql.schema.StarRocksField.OP;

public class StarRocksSchema implements Serializable {
    private List<StarRocksField> columns;
    private List<StarRocksField> keyColumns;
    private Map<String, StarRocksField> columnMap;

    private EtlJobConfig.EtlTable etlTable;
    private Long tableId;

    private Map<Long, TabletInfo> tabletId2StoragePathMap = new HashMap<>();

    public StarRocksSchema(List<StarRocksField> columns) {
        this(columns, new ArrayList<>(0));
    }

    // only use for schema
    public StarRocksSchema(List<StarRocksField> columns, List<StarRocksField> keyColumns) {
        this(columns, keyColumns, null, null);
    }

    public StarRocksSchema(List<StarRocksField> columns,
                           List<StarRocksField> keyColumns,
                           EtlJobConfig.EtlTable etlTable,
                           Long tableId) {
        this.columns = columns;
        this.keyColumns = keyColumns;
        this.columnMap = new HashMap<>();
        for (StarRocksField field : columns) {
            columnMap.put(field.getName(), field);
        }
        this.etlTable = etlTable;
        if (etlTable != null && etlTable.getPartitionInfo() != null) {
            for (EtlJobConfig.EtlPartition partition : etlTable.getPartitionInfo().getPartitions()) {
                for (int i = 0; i < partition.getTabletIds().size(); i++) {
                    tabletId2StoragePathMap.put(partition.getTabletIds().get(i),
                            new TabletInfo(partition.getStoragePath(), partition.getBackendIds().get(i)));
                }
            }
        }
        this.tableId = tableId;
    }

    public String getStoragePath(long tabletId) {
        return Optional.ofNullable(tabletId2StoragePathMap.get(tabletId))
                .map(TabletInfo::getStoragePath)
                .orElse("");
    }

    public long getBackendId(long tabletId) {
        Optional<TabletInfo> tabletInfo = Optional.ofNullable(tabletId2StoragePathMap.get(tabletId));
        return tabletInfo.map(TabletInfo::getBackendId).orElse(-1L);
    }

    public List<StarRocksField> getColumns() {
        return columns;
    }

    public boolean isPrimaryKey() {
        return !keyColumns.isEmpty();
    }

    public StarRocksField getField(String columnName) {
        if (OP.getName().equalsIgnoreCase(columnName)) {
            return OP;
        }

        return columnMap.get(columnName);
    }

    public EtlJobConfig.EtlTable getEtlTable() {
        return etlTable;
    }

    public Long getTableId() {
        return tableId;
    }

    public class TabletInfo implements Serializable {
        private String storagePath;
        private long backendId;

        public TabletInfo(String storagePath, long backendId) {
            this.storagePath = storagePath;
            this.backendId = backendId;
        }

        public String getStoragePath() {
            return storagePath;
        }

        public long getBackendId() {
            return backendId;
        }
    }

}
