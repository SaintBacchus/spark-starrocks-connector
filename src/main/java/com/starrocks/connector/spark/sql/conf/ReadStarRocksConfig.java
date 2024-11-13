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

package com.starrocks.connector.spark.sql.conf;

import java.util.Map;

public class ReadStarRocksConfig extends StarRocksConfigBase {

    private static final long serialVersionUID = 1L;

    public static final String USE_STARROCKS_CATALOG = "use.starrocks.catalog";

    public static final String KEY_READER_MODE = PREFIX + "reader.mode";

    public static final String FILTER_PUSHDOWN_ENABLED = PREFIX + "filter.pushdown.enabled";

    public ReadStarRocksConfig(Map<String, String> options) {
        super(options);
    }

    public enum ReadMode {

        RPC(0),

        BYPASS(1);

        private final int id;

        ReadMode(int id) {
            this.id = id;
        }

        public boolean is(String mode) {
            return this.name().equalsIgnoreCase(mode);
        }

        public boolean not(String mode) {
            return !is(mode);
        }

        public int getId() {
            return id;
        }
    }
}
