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

import org.apache.commons.lang3.StringUtils;

public class FieldUtils {

    public static String quoteIfNecessary(String fieldName) {
        if (StringUtils.isNotBlank(fieldName)) {
            String trimFn = fieldName.trim();
            if (trimFn.length() >= 2 && trimFn.startsWith("`") && trimFn.endsWith("`")) {
                return trimFn;
            }
        }

        return quote(fieldName);
    }

    public static String quote(String fieldName) {
        return null == fieldName ? null : String.format("`%s`", fieldName);
    }

    public static String eraseQuote(String fieldName) {
        if (StringUtils.isBlank(fieldName)) {
            return fieldName;
        }

        String trimFn = fieldName.trim();
        if (trimFn.length() >= 2 && trimFn.startsWith("`") && trimFn.endsWith("`")) {
            return trimFn.substring(1, trimFn.length() - 1);
        }

        return fieldName;
    }

}
