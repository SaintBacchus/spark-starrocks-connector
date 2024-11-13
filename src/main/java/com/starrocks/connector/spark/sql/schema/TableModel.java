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


import com.starrocks.proto.TabletSchema;

import java.util.Arrays;
import java.util.Optional;

public enum TableModel {

    DUPLICATE_KEY("DUPLICATE", "DUP_KEYS"),

    AGGREGATE_KEY("AGGREGATE", "AGG_KEYS"),

    UNIQUE_KEY("UNIQUE", "UNIQUE_KEYS"),

    PRIMARY_KEY("PRIMARY", "PRIMARY_KEYS");

    private final String id;

    private final String alias;

    TableModel(String id, String alias) {
        this.id = id;
        this.alias = alias;
    }

    public static boolean isValidType(String type) {
        return of(type).isPresent();
    }

    public static boolean notValidType(String type) {
        return !isValidType(type);
    }

    public boolean is(String type) {
        return this.getId().equalsIgnoreCase(type)
                || this.name().equalsIgnoreCase(type)
                || this.getAlias().equalsIgnoreCase(type);
    }

    public static Optional<TableModel> of(String type) {
        return Arrays.stream(values())
                .filter(model -> model.is(type))
                .findFirst();
    }

    public TabletSchema.KeysType toKeysType() {
        switch (this) {
            case DUPLICATE_KEY:
                return TabletSchema.KeysType.DUP_KEYS;
            case AGGREGATE_KEY:
                return TabletSchema.KeysType.AGG_KEYS;
            case UNIQUE_KEY:
                return TabletSchema.KeysType.UNIQUE_KEYS;
            case PRIMARY_KEY:
                return TabletSchema.KeysType.PRIMARY_KEYS;
            default:
                throw new IllegalStateException(
                        "can't convert '" + this.getId() + "' to " + TabletSchema.KeysType.class.getSimpleName());
        }
    }

    public String getId() {
        return id;
    }

    public String getAlias() {
        return alias;
    }
}
