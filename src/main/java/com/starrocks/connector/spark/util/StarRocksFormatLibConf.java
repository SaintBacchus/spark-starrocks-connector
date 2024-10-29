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

package com.starrocks.connector.spark.util;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class StarRocksFormatLibConf {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksFormatLibConf.class);
    private static final String FORMAT_LIB_CONF_PREFIX = "starrocks.formatlib.conf.";
    private Map<String, String> conf;
    public StarRocksFormatLibConf(Map<String, String> originOptions) {
        conf = originOptions.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FORMAT_LIB_CONF_PREFIX))
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().replaceFirst(FORMAT_LIB_CONF_PREFIX, "").replace(".", "_"),
                                Map.Entry::getValue
                        )
                );
        LOG.info("StarRocks format lib conf value: {}", conf);
    }

    @Override
    public String toString() {
        return conf.toString();
    }

    public void writeConf(String filePath) {
        writeSrFormatLibConf(filePath, this);
    }

    public static synchronized void writeSrFormatLibConf(String filePath, StarRocksFormatLibConf conf) {
        java.nio.file.Path path = Paths.get(filePath);
        try {
            if (Files.exists(path)) {
                LOG.info("Starrocks format lib conf {} exists, skip it", filePath);
                return;
            }
            StringBuilder content = new StringBuilder();
            for (Map.Entry<String, String> entry : conf.getConf().entrySet()) {
                content.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
            }
            Files.write(path, content.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            LOG.info("Starrocks format lib conf {} write success", filePath);
        } catch (Exception e) {
            LOG.error("Write Starrocks format lib conf {} failed, error is: ", conf, e);
            throw new RuntimeException(e);
        }
    }
}
