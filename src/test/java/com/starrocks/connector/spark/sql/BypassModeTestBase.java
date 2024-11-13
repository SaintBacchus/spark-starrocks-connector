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

import com.starrocks.connector.spark.ThrowingConsumer;
import com.starrocks.connector.spark.catalog.StarRocksCatalog;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.ReadMode;
import static com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.WriteMode;

public class BypassModeTestBase extends ITTestBase {

    protected static final String ENV_FORCE_CLEAN = "FORCE_CLEAN";

    public static Stream<Arguments> initMutableParams() {
        return Arrays.stream(ReadMode.values())
                .flatMap(readMode -> Arrays.stream(WriteMode.values())
                        .map(writeMode -> Arguments.of(true, readMode, writeMode)));
    }

    protected <E extends Exception> void withSparkSession(ThrowingConsumer<SparkSession, E> consumer)
            throws E {
        try (SparkSession sparkSession = getOrCreateSparkSession(
                builder -> builder, true, ReadMode.BYPASS, WriteMode.BYPASS)) {
            consumer.throwingAccept(sparkSession);
        }
    }

    protected <E extends Exception> void withSparkSession(ThrowingConsumer<SparkSession, E> consumer,
                                                          boolean useStarRocksCatalog,
                                                          ReadMode readMode,
                                                          WriteMode writeMode) throws E {
        try (SparkSession sparkSession = getOrCreateSparkSession(
                builder -> builder, useStarRocksCatalog, readMode, writeMode)) {
            consumer.throwingAccept(sparkSession);
        }
    }

    protected <E extends Exception> void withSparkSession(Function<SparkSession.Builder, SparkSession.Builder> function,
                                                          ThrowingConsumer<SparkSession, E> consumer) throws E {
        withSparkSession(function, consumer, true, ReadMode.BYPASS, WriteMode.BYPASS);
    }

    protected <E extends Exception> void withSparkSession(Function<SparkSession.Builder, SparkSession.Builder> function,
                                                          ThrowingConsumer<SparkSession, E> consumer,
                                                          boolean useStarRocksCatalog,
                                                          ReadMode readMode,
                                                          WriteMode writeMode) throws E {
        try (SparkSession sparkSession = getOrCreateSparkSession(function, useStarRocksCatalog, readMode, writeMode)) {
            consumer.throwingAccept(sparkSession);
        }
    }

    protected <E extends Exception> void withSparkSession(Supplier<SparkSession.Builder> supplier,
                                                          ThrowingConsumer<SparkSession, E> consumer) throws E {
        withSparkSession(supplier, builder -> builder, consumer);
    }

    protected <E extends Exception> void withSparkSession(Supplier<SparkSession.Builder> supplier,
                                                          Function<SparkSession.Builder, SparkSession.Builder> function,
                                                          ThrowingConsumer<SparkSession, E> consumer) throws E {
        try (SparkSession sparkSession = getOrCreateSparkSession(supplier, function)) {
            consumer.throwingAccept(sparkSession);
        }
    }

    protected static SparkSession getOrCreateSparkSession(Function<SparkSession.Builder, SparkSession.Builder> function,
                                                          boolean useStarRocksCatalog,
                                                          ReadMode readMode,
                                                          WriteMode writeMode) {
        return getOrCreateSparkSession(() -> {
                    SparkSession.Builder builder = SparkSession.builder()
                            .master("local[1]")
                            .appName(BypassReadWriteTest.class.getSimpleName())
                            /* for dev */
                            .config("spark.sql.codegen.wholeStage", false)
                            .config("spark.sql.codegen.factoryMode", "NO_CODEGEN")
                            .config("spark.sql.catalog.starrocks.verbose.enabled", true)
                            .config("spark.sql.catalog.starrocks.fe.http.url", FE_HTTP)
                            .config("spark.sql.catalog.starrocks.fe.jdbc.url", FE_JDBC)
                            .config("spark.sql.catalog.starrocks.user", USER)
                            .config("spark.sql.catalog.starrocks.password", PASSWORD)
                            .config("spark.sql.catalog.starrocks.request.tablet.size", 1);


                    if (useStarRocksCatalog) {
                        builder.config("spark.sql.defaultCatalog", "starrocks")
                                .config("spark.sql.catalog.starrocks", StarRocksCatalog.class.getCanonicalName());
                    }

                    if (ReadMode.BYPASS.equals(readMode) || WriteMode.BYPASS.equals(writeMode)) {
                        builder.config("spark.sql.extensions", "com.starrocks.connector.spark.StarRocksExtensions")
                                .config("spark.sql.catalog.starrocks.fs.s3a.endpoint", S3_ENDPOINT)
                                .config("spark.sql.catalog.starrocks.fs.s3a.endpoint.region", S3_REGION)
                                .config("spark.sql.catalog.starrocks.fs.s3a.connection.ssl.enabled", true)
                                .config("spark.sql.catalog.starrocks.fs.s3a.path.style.access", false)
                                .config("spark.sql.catalog.starrocks.fs.s3a.access.key", S3_AK)
                                .config("spark.sql.catalog.starrocks.fs.s3a.secret.key", S3_SK);

                        Optional.ofNullable(readMode).ifPresent(
                                mode -> builder.config("spark.sql.catalog.starrocks.reader.mode", mode.name())
                        );

                        Optional.ofNullable(writeMode).ifPresent(
                                mode -> builder
                                        .config("spark.sql.catalog.starrocks.writer.mode", mode.name())
                                        .config("spark.sql.catalog.starrocks.write.buffer.size", 10)
                        );

                    }

                    return builder;
                },
                function);
    }

    protected static SparkSession getOrCreateSparkSession(Supplier<SparkSession.Builder> supplier,
                                                          Function<SparkSession.Builder, SparkSession.Builder> function) {
        return function.apply(supplier.get()).getOrCreate();
    }

    protected static String loadSql(Table table) throws IOException {
        return String.format(
                loadSqlTemplate("sql/" + table.getSqlTemplateName() + ".sql"), DB_NAME, table.name().toLowerCase()
        );
    }

    protected static void clean(String[] tables) throws Exception {
        clean(tables, BooleanUtils.TRUE.equalsIgnoreCase(System.getenv(ENV_FORCE_CLEAN)));
    }

    protected static void clean(String[] tables, boolean forcible) throws Exception {
        String forceTag = forcible ? "FORCE" : "";
        try {
            executeSrSQL(String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME));
            for (String table : tables) {
                executeSrSQL(String.format("DROP TABLE IF EXISTS `%s`.`%s` %s", DB_NAME, table, forceTag));
            }
        } finally {
            executeSrSQL(String.format("DROP DATABASE IF EXISTS `%s` %s", DB_NAME, forceTag));
        }
    }

}
