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

package com.starrocks.connector.spark;

import org.apache.arrow.vector.types.Types;

import java.util.Arrays;
import java.util.Optional;

public enum DataType {

    NULL("NULL_TYPE"),
    BOOLEAN("BOOLEAN"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    INT("INT"),
    BIGINT("BIGINT"),
    LARGEINT("LARGEINT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DATE("DATE"),
    TIME("TIME"),
    DATETIME("DATETIME"),
    CHAR("CHAR"),
    VARCHAR("VARCHAR"),
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    DECIMAL("DECIMAL"),
    DECIMALV2("DECIMALV2"),
    DECIMAL32("DECIMAL32"),
    DECIMAL64("DECIMAL64"),
    DECIMAL128("DECIMAL128"),
    BITMAP("BITMAP"),
    HLL("HLL"),
    JSON("JSON"),
    ARRAY("ARRAY"),
    STRUCT("STRUCT"),
    MAP("MAP");

    public static final long MILLIS_OF_DAY = 24 * 60 * 60 * 1000L;

    private final String alias;

    DataType(String alias) {
        this.alias = alias;
    }

    public static DataType of(String typeName) {
        return elegantOf(typeName)
                .orElseThrow(() -> new IllegalArgumentException("Unknown field type: " + typeName));
    }

    public static Optional<DataType> elegantOf(String typeName) {
        return Arrays.stream(values())
                .filter(type -> type.name().equalsIgnoreCase(typeName)
                        || type.getAlias().equalsIgnoreCase(typeName))
                .findFirst();
    }

    public static DataType fromArrowMinorType(Types.MinorType minorType) {
        switch (minorType) {
            case NULL:
                return NULL;
            case BIT:
                return BOOLEAN;
            case TINYINT:
                return TINYINT;
            case SMALLINT:
                return SMALLINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case FLOAT4:
                return FLOAT;
            case FLOAT8:
                return DOUBLE;
            case DECIMAL:
                return DECIMAL128;
            case VARCHAR:
                return VARCHAR;
            case VARBINARY:
                return BINARY;
            case STRUCT:
                return STRUCT;
            case LIST:
                return ARRAY;
            case MAP:
                return MAP;
            default:
                throw new IllegalStateException("Unsupported arrow minor type: " + minorType);
        }
    }

    public static boolean isSupported(String typeName) {
        return elegantOf(typeName).isPresent();
    }

    public static boolean notSupported(String typeName) {
        return !isSupported(typeName);
    }

    public String getAlias() {
        return alias;
    }
}
