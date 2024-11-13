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

import com.starrocks.connector.spark.DataType;
import com.starrocks.format.rest.model.Column;

import java.io.Serializable;
import java.util.StringJoiner;


public class StarRocksField implements Serializable {

    private static final long serialVersionUID = -3617841413720459202L;

    public static final StarRocksField OP = new StarRocksField(
            "__op",
            new Column.Type() {

                private static final long serialVersionUID = -2875972354443981275L;

                {
                    setName(DataType.TINYINT.name());
                    setTypeSize(3);
                }
            },
            Integer.MAX_VALUE,
            true
    );

    private final String name;
    private final Column.Type type;
    private final int ordinalPosition;
    private final boolean nullable;

    public StarRocksField(String name, Column.Type type) {
        this(name, type, -1, true);
    }

    public StarRocksField(String name,
                          Column.Type type,
                          int ordinalPosition,
                          boolean nullable) {
        this.name = name;
        this.type = type;
        this.ordinalPosition = ordinalPosition;
        this.nullable = nullable;
    }

    public boolean isBitmap() {
        return "bitmap".equalsIgnoreCase(getType().getName());
    }

    public boolean isHll() {
        return "hll".equalsIgnoreCase(getType().getName());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", "[", "]")
                .add("name='" + name + "'")
                .add("type=" + type)
                .add("ordinalPosition=" + ordinalPosition)
                .add("nullable=" + nullable)
                .toString();
    }

    public String getName() {
        return name;
    }

    public Column.Type getType() {
        return type;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public boolean isNullable() {
        return nullable;
    }

}
