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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public abstract class ITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(ITTestBase.class);

    protected static String FE_HTTP = "127.0.0.1:11901";
    protected static String FE_JDBC = "jdbc:mysql://127.0.0.1:11903";
    protected static String USER = "root";
    protected static String PASSWORD = "";

    protected static String S3_ENDPOINT = "https://tos-s3-cn-beijing.ivolces.com";
    protected static String S3_REGION = "cn-beijing";
    protected static String S3_AK;
    protected static String S3_SK;

    private static final boolean DEBUG_MODE = false;
    protected static final String DB_NAME = "sr_spark_test_db_" + RandomStringUtils.randomAlphabetic(8).toLowerCase();

    protected static Connection DB_CONNECTION;

    protected static final ObjectMapper JSON = new ObjectMapper();

    @BeforeAll
    public static void beforeClass() throws Exception {
        Properties props = loadConnProps();
        FE_HTTP = props.getProperty("starrocks.fe.http.url", FE_HTTP);
        FE_JDBC = props.getProperty("starrocks.fe.jdbc.url", FE_JDBC);
        USER = props.getProperty("starrocks.user", USER);
        PASSWORD = props.getProperty("starrocks.password", PASSWORD);

        S3_ENDPOINT = props.getProperty("starrocks.fs.s3.endpoint", S3_ENDPOINT);
        S3_REGION = props.getProperty("starrocks.fs.s3.region", S3_REGION);
        S3_AK = props.getProperty("starrocks.fs.s3.accessKey");
        S3_SK = props.getProperty("starrocks.fs.s3.secretKey");

        try {
            DB_CONNECTION = DriverManager.getConnection(FE_JDBC, USER, PASSWORD);
            LOG.info("Success to create db connection via jdbc {}", FE_JDBC);
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", FE_JDBC, e);
            throw e;
        }

        try {
            executeSrSQL(String.format("CREATE DATABASE IF NOT EXISTS %s", DB_NAME));
            LOG.info("Successful to create database {}", DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", DB_NAME, e);
            throw e;
        }
    }

    protected static Properties loadConnProps() throws IOException {
        try (InputStream inputStream = ITTestBase.class.getClassLoader()
                .getResourceAsStream("starrocks_conn.properties")) {
            Properties props = new Properties();
            if (null == inputStream) {
                System.out.println("WARNING: starrocks_conn.properties not found.");
                return props;
            }
            props.load(inputStream);
            return props;
        }
    }

    protected static String loadSqlTemplate(String filepath) throws IOException {
        try (InputStream inputStream = ITTestBase.class.getClassLoader().getResourceAsStream(filepath)) {
            return IOUtils.toString(
                    requireNonNull(inputStream, "null input stream when load '" + filepath + "'"),
                    StandardCharsets.UTF_8);
        }
    }

    @AfterAll
    public static void afterClass() throws Exception {
        if (DB_CONNECTION != null) {
            try {
                executeSrSQL(String.format("DROP DATABASE IF EXISTS %s FORCE", DB_NAME));
                LOG.info("Successful to drop database {}", DB_NAME);
            } catch (Exception e) {
                LOG.error("Failed to drop database {}", DB_NAME, e);
            }
            DB_CONNECTION.close();
        }
    }

    protected static String genRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    protected static void executeSrSQL(String sql) throws Exception {
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            statement.execute();
        }
    }

    protected static String addHttpSchemaPrefix(String feHosts, String prefix) {
        String[] hosts = feHosts.split(",");
        return Arrays.stream(hosts).map(h -> prefix + h).collect(Collectors.joining(","));
    }

    protected static List<List<Object>> scanTable(Connection dbConnector, String db, String table) throws SQLException {
        String query = String.format("SELECT * FROM `%s`.`%s`", db, table);
        return queryTable(dbConnector, query);
    }

    protected static List<List<Object>> queryTable(Connection dbConnector, String query) throws SQLException {
        try (PreparedStatement statement = dbConnector.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                List<List<Object>> results = new ArrayList<>();
                int numColumns = resultSet.getMetaData().getColumnCount();
                while (resultSet.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= numColumns; i++) {
                        row.add(resultSet.getObject(i));
                    }
                    results.add(row);
                }
                return results;
            }
        }
    }

    private static final SimpleDateFormat DATETIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected static void verifyRows(List<List<Object>> expected, List<Row> actualRows) {
        List<List<Object>> actual = new ArrayList<>();
        for (Row row : actualRows) {
            List<Object> objects = new ArrayList<>();
            for (int i = 0; i < row.length(); i++) {
                objects.add(row.get(i));
            }
            actual.add(objects);
        }

        verifyResult(expected, actual);
    }

    protected static void verifyResult(List<List<Object>> expected, List<List<Object>> actual) {
        List<String> expectedRows = new ArrayList<>();
        for (List<Object> row : expected) {
            StringJoiner joiner = new StringJoiner(",");
            for (Object col : row) {
                joiner.add(format(col, false));
            }
            expectedRows.add(joiner.toString());
        }
        expectedRows.sort(String::compareTo);

        List<String> actualRows = new ArrayList<>();
        for (List<Object> row : actual) {
            StringJoiner joiner = new StringJoiner(",");
            for (Object col : row) {
                joiner.add(format(col, false));
            }
            actualRows.add(joiner.toString());
        }
        actualRows.sort(String::compareTo);
        System.out.println("Expected: " + expectedRows);
        System.out.println("Actually: " + actualRows);
        assertArrayEquals(expectedRows.toArray(), actualRows.toArray());
    }

    private static String format(Object object, boolean quoted) {
        String prev;
        if (object instanceof List) {
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            for (Object obj : ((List<?>) object)) {
                joiner.add(format(obj, true));
            }
            prev = joiner.toString();
        } else if (object instanceof WrappedArray) {
            StringJoiner joiner = new StringJoiner(",", "[", "]");
            for (Object obj : JavaConverters.seqAsJavaList(((WrappedArray<?>) object))) {
                joiner.add(format(obj, quoted));
            }
            prev = joiner.toString();
        } else if (object instanceof Timestamp) {
            prev = DATETIME_FORMATTER.format((Timestamp) object);
        } else if (object instanceof GenericRowWithSchema) {
            GenericRowWithSchema row = (GenericRowWithSchema) object;
            StructType schema = row.schema();
            StringJoiner joiner = new StringJoiner(",", "{", "}");
            for (int i = 0; i < schema.fields().length; i++) {
                StructField field = schema.fields()[i];
                joiner.add(String.format("\"%s\":%s", field.name(), format(row.values()[i], quoted)));
            }
            prev = joiner.toString();
        } else if (object instanceof scala.collection.Map) {
            Map<Object, Object> map = JavaConverters.mapAsJavaMap((scala.collection.Map<Object, Object>) object);
            StringJoiner joiner = new StringJoiner(",", "{", "}");
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                joiner.add(String.format("%s:%s", format(entry.getKey(), quoted), format(entry.getValue(), quoted)));
            }
            prev = joiner.toString();
        } else {
            prev = object == null ? "null" : object.toString();
        }

        if (quoted && (object instanceof String || object instanceof Date)) {
            return String.format("\"%s\"", prev);
        } else {
            return prev;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    protected static class ActionLog {

        @JsonProperty("act_time")
        private Long actTime;

        @JsonProperty("user_id")
        private Long userId;

        @JsonProperty("act_type")
        private String actType;

        @JsonProperty("status")
        private String status;

        @JsonProperty("user_agent")
        private String userAgent;

        public ActionLog() {
        }

        public ActionLog(Long actTime, Long userId, String actType, String status) {
            this.actTime = actTime;
            this.userId = userId;
            this.actType = actType;
            this.status = status;
        }

        public ActionLog(Long actTime, Long userId, String actType, String status, String userAgent) {
            this.actTime = actTime;
            this.userId = userId;
            this.actType = actType;
            this.status = status;
            this.userAgent = userAgent;
        }

        @Override
        public String toString() {
            try {
                return JSON.writeValueAsString(this);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ActionLog actionLog = (ActionLog) o;
            return Objects.equals(getActTime(), actionLog.getActTime()) &&
                    Objects.equals(getUserId(), actionLog.getUserId()) &&
                    Objects.equals(getActType(), actionLog.getActType()) &&
                    Objects.equals(getStatus(), actionLog.getStatus()) &&
                    Objects.equals(getUserAgent(), actionLog.getUserAgent());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getActTime(), getUserId(), getActType(), getStatus(), getUserAgent());
        }

        public Long getActTime() {
            return actTime;
        }

        public void setActTime(Long actTime) {
            this.actTime = actTime;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public String getActType() {
            return actType;
        }

        public void setActType(String actType) {
            this.actType = actType;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getUserAgent() {
            return userAgent;
        }

        public void setUserAgent(String userAgent) {
            this.userAgent = userAgent;
        }
    }

}
