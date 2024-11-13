// Modifications Copyright 2021 StarRocks Limited.
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

package com.starrocks.connector.spark.cfg;

import com.google.common.base.Preconditions;
import org.apache.spark.SparkConf;
import scala.Option;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.makeWriteCompatibleWithRead;

public class SparkSettings extends Settings {

    public static final String SPARK_SQL_OPTION_PREFIX = "spark.sql.catalog.";
    public static final int SPARK_SQL_OPTION_PREFIX_LEN = SPARK_SQL_OPTION_PREFIX.length();

    private final SparkConf cfg;

    public SparkSettings() {
        this(new SparkConf(true));
    }

    public SparkSettings(SparkConf cfg) {
        Preconditions.checkArgument(cfg != null, "non-null spark configuration expected.");
        Map<String, String> srConfigMap = new HashMap<>();
        for (Tuple2<String, String> tuple : cfg.getAllWithPrefix(SPARK_SQL_OPTION_PREFIX)) {
            srConfigMap.putIfAbsent(tuple._1.substring(SPARK_SQL_OPTION_PREFIX_LEN), tuple._2);
        }

        srConfigMap = makeWriteCompatibleWithRead(srConfigMap);
        srConfigMap.forEach(cfg::setIfMissing);

        this.cfg = cfg;
    }

    @Override
    public SparkSettings copy() {
        return new SparkSettings(cfg.clone());
    }

    @Override
    public String getProperty(String name) {
        Option<String> op = cfg.getOption(name);
        if (!op.isDefined()) {
            op = cfg.getOption("spark." + name);
        }
        return (op.isDefined() ? op.get() : null);
    }

    @Override
    public void setProperty(String name, String value) {
        cfg.set(name, value);
    }

    @Override
    public void removeProperty(String name) {
        cfg.remove(name);
    }

    @Override
    public Properties asProperties() {
        Properties props = new Properties();

        if (cfg != null) {
            String sparkPrefix = "spark.";
            for (Tuple2<String, String> tuple : cfg.getAll()) {
                // spark. are special so save them without the prefix as well
                // since its unlikely the other implementations will be aware of this convention
                String key = tuple._1;
                props.setProperty(key, tuple._2);
                if (key.startsWith(sparkPrefix)) {
                    String simpleKey = key.substring(sparkPrefix.length());
                    // double check to not override a property defined directly in the config
                    if (!props.containsKey(simpleKey)) {
                        props.setProperty(simpleKey, tuple._2);
                    }
                }
            }
        }

        return props;
    }

    public static String addPrefix(String optionName) {
        if (optionName.startsWith(SPARK_SQL_OPTION_PREFIX)) {
            return optionName;
        }
        return SPARK_SQL_OPTION_PREFIX + optionName;
    }

}
