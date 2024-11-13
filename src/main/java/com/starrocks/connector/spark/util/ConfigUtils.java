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

import com.starrocks.connector.spark.cfg.Settings;
import com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.ReadMode;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.WriteMode;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.util.Map;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TIMEZONE;
import static com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.FILTER_PUSHDOWN_ENABLED;
import static com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.KEY_READER_MODE;
import static com.starrocks.connector.spark.sql.conf.ReadStarRocksConfig.USE_STARROCKS_CATALOG;
import static com.starrocks.connector.spark.sql.conf.StarRocksConfigBase.KEY_VERBOSE_ENABLED;
import static com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.KEY_WRITER_MODE;

public class ConfigUtils {

    public static boolean isVerbose(Settings settings) {
        return BooleanUtils.toBoolean(settings.getProperty(KEY_VERBOSE_ENABLED, Boolean.FALSE.toString()));
    }

    public static boolean isBypass(Map<String, String> options) {
        return ReadMode.BYPASS.is(options.get(KEY_READER_MODE))
                || WriteMode.BYPASS.is(options.get(KEY_WRITER_MODE));
    }

    public static boolean notBypass(Map<String, String> options) {
        return !isBypass(options);
    }

    public static boolean isBypassRead(Settings settings) {
        return ReadMode.BYPASS.is(settings.getProperty(KEY_READER_MODE));
    }

    public static boolean notBypassRead(Settings settings) {
        return !isBypassRead(settings);
    }

    public static boolean useStarRocksCatalog(Settings settings) {
        return BooleanUtils.toBoolean(settings.getProperty(USE_STARROCKS_CATALOG, Boolean.FALSE.toString()));
    }

    public static boolean isFilterPushDownEnabled(Settings settings) {
        return BooleanUtils.toBoolean(settings.getProperty(FILTER_PUSHDOWN_ENABLED, Boolean.TRUE.toString()));
    }

    public static ZoneId resolveTimeZone(Settings settings) {
        String tz = settings.getProperty(STARROCKS_TIMEZONE);
        if (StringUtils.isBlank(tz)) {
            return ZoneId.systemDefault();
        }
        return ZoneId.of(tz);
    }

    private ConfigUtils() {
    }

}
