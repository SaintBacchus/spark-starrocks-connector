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
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class StarRocksConnectorTest {

    @Test
    public void testLoadSegmentData() {
        StarRocksConnector srConnector = new StarRocksConnector("jdbc:mysql://127.0.0.1:9030/tpcds", "root", "");
        srConnector.loadSegmentData("tpcds", "aa", "s3://bucket1/.staging/", "reason", "minio_access_key", "minio_secret_key", "http://127.0.0.1:9000");
    }

    @Test
    public void testWaitLoadSegmentData() {
        StarRocksConnector srConnector = new StarRocksConnector("jdbc:mysql://127.0.0.1:9030/tpcds", "root", "");
        List<Map<String, String>> loads = srConnector.getSegmentLoadState("tpcds", "aa");
        System.out.println(loads.toString());
    }
}
