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

package com.starrocks.connector.spark.sql.connect;

import com.starrocks.connector.spark.exception.ConnectedFailedException;
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.sql.schema.TableIdentifier;
import com.starrocks.format.rest.model.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StarRocksConnector implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksConnector.class);

    private static final String TABLE_SCHEMA_QUERY = "SELECT " +
            "       `COLUMN_NAME`," +
            "       `ORDINAL_POSITION`," +
            "       `IS_NULLABLE`," +
            "       `COLUMN_KEY`," +
            "       `DATA_TYPE`," +
            "       `COLUMN_SIZE`," +
            "       `DECIMAL_DIGITS`," +
            "       `NUMERIC_PRECISION`," +
            "       `NUMERIC_SCALE`" +
            " FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

    private static final String ALL_DBS_QUERY = "show databases;";
    private static final String LOAD_DB_QUERY =
            "select SCHEMA_NAME from information_schema.schemata where SCHEMA_NAME in (?);";
    private static final String ALL_TABLES_QUERY = "select TABLE_SCHEMA, TABLE_NAME from information_schema.tables " +
            "where TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA in (?) ;";

    // Driver name for mysql connector 5.1 which is deprecated in 8.0
    private static final String MYSQL_51_DRIVER_NAME = "com.mysql.jdbc.Driver";
    // Driver name for mysql connector 8.0
    private static final String MYSQL_80_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_SITE_URL = "https://dev.mysql.com/downloads/connector/j/";
    private static final String MAVEN_CENTRAL_URL = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public StarRocksConnector(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public StarRocksSchema getSchema(TableIdentifier identifier) {
        List<String> parameters = Arrays.asList(identifier.getDatabase(), identifier.getTable());
        List<Map<String, String>> columnValues = extractColumnValuesBySql(TABLE_SCHEMA_QUERY, parameters);

        List<StarRocksField> pks = new ArrayList<>();
        List<StarRocksField> columns = new ArrayList<>();
        for (Map<String, String> columnValue : columnValues) {
            StarRocksField field = new StarRocksField(
                    columnValue.get("COLUMN_NAME"),
                    new Column.Type() {

                        private static final long serialVersionUID = 163681101155432617L;

                        {
                            setName(columnValue.get("DATA_TYPE"));
                            setColumnSize(
                                    Optional.ofNullable(columnValue.get("COLUMN_SIZE")).map(Integer::parseInt).orElse(null)
                            );
                            setPrecision(
                                    Optional.ofNullable(columnValue.get("NUMERIC_PRECISION")).map(Integer::parseInt).orElse(null)
                            );
                            // FIXME can replace DECIMAL_DIGITS to NUMERIC_SCALE for decimal?
                            setScale(
                                    Optional.ofNullable(columnValue.get("NUMERIC_SCALE")).map(Integer::parseInt).orElse(null)
                            );
                        }
                    },
                    Integer.parseInt(columnValue.get("ORDINAL_POSITION")),
                    "YES".equalsIgnoreCase(columnValue.get("IS_NULLABLE"))
            );
            columns.add(field);
            if ("PRI".equals(columnValue.get("COLUMN_KEY"))) {
                pks.add(field);
            }
        }
        columns.sort(Comparator.comparingInt(StarRocksField::getOrdinalPosition));

        return new StarRocksSchema(columns, pks);
    }

    public List<String> getDatabases() {
        List<Map<String, String>> dbs = extractColumnValuesBySql(ALL_DBS_QUERY, new ArrayList<>(0));

        List<String> dbNames = new ArrayList<>();
        for (Map<String, String> db : dbs) {
            String dbName = Optional.ofNullable(db.get("Database"))
                    .orElseThrow(() -> new StarRocksException("get Database header error"));
            dbNames.add(dbName);
        }

        return dbNames;
    }

    public Map<String, String> loadDatabase(List<String> dbNames) {
        String inClause = String.join(", ", dbNames);
        List<String> parameters = Collections.singletonList(inClause);
        List<Map<String, String>> dbs = extractColumnValuesBySql(LOAD_DB_QUERY, parameters);

        for (Map<String, String> db : dbs) {
            String dbName = Optional.ofNullable(db.get("SCHEMA_NAME"))
                    .orElseThrow(() -> new StarRocksException("get Database SCHEMA_NAME error"));
            return new DatabaseSpec(dbName).toJavaMap();
        }

        throw new StarRocksException("database(s) not found: " + inClause);
    }

    public Map<String, String> getTables(List<String> dbNames) {
        List<String> parameters = Collections.singletonList(String.join(",", dbNames));
        List<Map<String, String>> tables = extractColumnValuesBySql(ALL_TABLES_QUERY, parameters);
        Map<String, String> table2Db = new HashMap<>();

        for (Map<String, String> db : tables) {
            String dbName = Optional.ofNullable(db.get("TABLE_SCHEMA"))
                    .orElseThrow(() -> new StarRocksException("get table header error"));
            String tableName = Optional.ofNullable(db.get("TABLE_NAME"))
                    .orElseThrow(() -> new StarRocksException("get table header error"));

            table2Db.put(tableName, dbName);
        }

        return table2Db;
    }

    private List<Map<String, String>> extractColumnValuesBySql(String sqlPattern, List<String> parameters) {
        List<Map<String, String>> columnValues = new ArrayList<>();
        try (
                // FIXME reuse connection? by zhenchao 2024-08-09 11:56:46
                Connection conn = createJdbcConnection();
                PreparedStatement ps = conn.prepareStatement(sqlPattern)
        ) {
            for (int i = 1; i <= parameters.size(); i++) {
                ps.setObject(i, parameters.get(i - 1));
            }

            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    // colName -> colValue
                    row.put(metaData.getColumnName(i), rs.getString(i));
                }
                columnValues.add(row);
            }
            rs.close();
        } catch (Exception e) {
            if (e instanceof IllegalStateException) {
                throw (IllegalStateException) e;
            }
            throw new IllegalStateException("extract column values by sql error, " + e.getMessage(), e);
        }

        return columnValues;
    }

    private Connection createJdbcConnection() {
        try {
            Class.forName(MYSQL_80_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            try {
                Class.forName(MYSQL_51_DRIVER_NAME);
            } catch (ClassNotFoundException ie) {
                String msg = String.format("Can't find mysql jdbc driver, please download it and " +
                                "put it in your classpath manually. Note that the connector does not include " +
                                "the mysql driver since version 1.1.1 because of the limitation of GPL license " +
                                "used by the driver. You can download it from MySQL site %s, or Maven Central %s",
                        MYSQL_SITE_URL, MAVEN_CENTRAL_URL);
                throw new StarRocksException(msg);
            }
        }

        try {
            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            LOG.error("Connect to {} with user {} error.", jdbcUrl, username, e);
            throw new ConnectedFailedException(jdbcUrl, e);
        }
    }

}
