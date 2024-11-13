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

package com.starrocks.connector.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StarRocksCatalogTest extends BypassModeTestBase {

    @BeforeEach
    public void beforeEach() throws Exception {
        ITTestBase.beforeClass();
        clean(Arrays.stream(Table.values()).map(Table::name).toArray(String[]::new));
    }

    @AfterEach
    public void afterEach() throws Exception {
        clean(Arrays.stream(Table.values()).map(Table::name).toArray(String[]::new));
        ITTestBase.afterClass();
    }

    @ParameterizedTest
    @MethodSource("initMutableParams")
    public void testCatalog() throws Exception {
        withSparkSession(spark -> {
            {
                Dataset<Row> df = spark.sql("SHOW DATABASES");
                df.show();
                List<Row> rows = df.collectAsList();
                Assertions.assertTrue(rows.stream().noneMatch(r -> DB_NAME.equals(r.getAs("namespace"))));
            }

            {
                executeSrSQL(String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME));
                Dataset<Row> df = spark.sql("SHOW DATABASES");
                df.show();
                List<Row> rows = df.collectAsList();
                Assertions.assertTrue(rows.stream().anyMatch(r -> DB_NAME.equals(r.getAs("namespace"))));
            }

            spark.sql(String.format("USE starrocks.%s", DB_NAME));

            {
                Dataset<Row> df = spark.sql("SHOW TABLES");
                df.show();
                List<Row> rows = df.collectAsList();
                Assertions.assertEquals(0, rows.size());
            }

            for (Table table : Table.values()) {
                executeSrSQL(loadSql(table));
            }

            {
                Dataset<Row> df = spark.sql("SHOW TABLES");
                df.show();
                List<Row> rows = df.collectAsList();
                Assertions.assertArrayEquals(
                        Arrays.stream(Table.values()).map(Table::getTableName).sorted(String::compareTo).toArray(String[]::new),
                        rows.stream()
                                .map(row -> Objects.toString(row.getAs("tableName")))
                                .sorted(String::compareTo).toArray(String[]::new));
            }
        });
    }
}
