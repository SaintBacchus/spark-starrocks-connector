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


import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.connector.catalog.Identifier;

import java.io.Serializable;
import java.util.Objects;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_DEFAULT_CATALOG;

public class TableIdentifier implements Serializable {

    private static final long serialVersionUID = 6427972464743834988L;

    private final String catalog;

    private final String database;

    private final String table;

    public TableIdentifier(String database, String table) {
        this(STARROCKS_DEFAULT_CATALOG, database, table);
    }

    public TableIdentifier(String catalog, String database, String table) {
        this.catalog = StringUtils.trimToEmpty(catalog);
        this.database = StringUtils.trimToEmpty(database);
        this.table = StringUtils.trimToEmpty(table);
    }

    public static TableIdentifier createFrom(Identifier identifier) {
        return new TableIdentifier(identifier.namespace()[0], identifier.name());
    }

    public static TableIdentifier createFrom(String fullName) {
        if (StringUtils.isBlank(fullName)) {
            throw new IllegalArgumentException("invalid fullName: " + fullName);
        }

        String[] elems = StringUtils.trimToEmpty(fullName).split("\\.");
        if (2 == elems.length) {
            return new TableIdentifier(elems[0], elems[1]);
        }

        if (3 == elems.length) {
            return new TableIdentifier(elems[0], elems[1], elems[2]);
        }

        throw new IllegalArgumentException("invalid fullName: " + fullName);
    }

    public String toFullName() {
        return String.format("%s.%s", database, table);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TableIdentifier that = (TableIdentifier) obj;
        return Objects.equals(getCatalog(), that.getCatalog())
                && Objects.equals(getDatabase(), that.getDatabase())
                && Objects.equals(getTable(), that.getTable());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCatalog(), getDatabase(), getTable());
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", catalog, database, table);
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }
}
