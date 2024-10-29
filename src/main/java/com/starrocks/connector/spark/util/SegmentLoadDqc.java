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

import com.starrocks.connector.spark.rest.models.FieldType;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SegmentLoadDqc {
    public static final Long DEFAULT_TABLET_ID = -1L;
    private long rows = 0;
    // for double type, value is Double,
    // for float type, value is Float,
    // others, value is Long
    private Map<String, Object> dqcResMap = new HashMap<>();
    private Map<String, Integer> dqcColumns = new HashMap<>();
    private Long tableId = DEFAULT_TABLET_ID;

    public Long getTableId() {
        return this.tableId;
    }

    public void setTableId(Long tabletId) {
        this.tableId = tabletId;
    }

    public void reset() {
        dqcColumns.clear();
        dqcResMap.clear();
        rows = 0;
    }

    @Override
    public String toString() {
        return "SegmengLoadDqc{" +
                "rows=" + rows +
                ", dqcResMap=" + dqcResMap +
                '}';
    }

    public JSONObject toJsonObject() {
        JSONObject res = new JSONObject();
        if (!DEFAULT_TABLET_ID.equals(tableId)) {
            res.put("tablet_id", tableId);
        }
        res.put("rows", rows);
        JSONArray dqcArray = new JSONArray();
        for (Map.Entry<String, Object> entry : dqcResMap.entrySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("key", entry.getKey());
            jsonObject.put("value", entry.getValue());
            dqcArray.put(jsonObject);
        }
        res.put("columns", dqcArray);
        return res;
    }

    public void increase() {
        rows++;
    }

    // collect columns for dqc
    public void collectDqcColumn(int colIndex, StarRocksSchema schema) {
        StarRocksField field = schema.getColumns().get(colIndex);
        String type = field.getType();
        if (dqcColumns.containsKey(type)) {
            return;
        }
        dqcColumns.put(type, colIndex);
    }

    // for float、double，only calculate the sum of these numbers
    // others, calculate the sum of these value's hash value
    public void dqcProcess(InternalRow internalRow, StarRocksSchema schema) {
        for (Map.Entry<String, Integer> entry : dqcColumns.entrySet()) {
            StarRocksField field = schema.getColumns().get(entry.getValue());
            int colIndex = entry.getValue();
            // just skip NULL
            if (internalRow.isNullAt(colIndex)) {
                continue;
            }
            String colName = field.getName();
            String currentStrValue = "";
            switch (FieldType.of(entry.getKey())) {
                case BOOLEAN:
                    currentStrValue = internalRow.getBoolean(colIndex) ? "1" : "0";
                    break;
                case TINYINT:
                case INT:
                    currentStrValue = String.valueOf(internalRow.getInt(colIndex));
                    break;
                case BIGINT:
                    currentStrValue = String.valueOf(internalRow.getLong(colIndex));
                    break;
                case FLOAT:
                    Float floatSum = (Float) dqcResMap.getOrDefault(colName, 0f);
                    dqcResMap.put(colName, floatSum + internalRow.getFloat(colIndex));
                    continue;
                case DOUBLE:
                    Double doubleSum = (Double) dqcResMap.getOrDefault(colName, 0d);
                    dqcResMap.put(colName, doubleSum + internalRow.getDouble(colIndex));
                    continue;
                case DECIMAL:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    currentStrValue = internalRow
                            .getDecimal(colIndex, field.getPrecision(), field.getScale()).toJavaBigDecimal().toString();
                    break;
                case CHAR:
                case VARCHAR:
                    currentStrValue = internalRow.getUTF8String(colIndex).toString();
                    break;
                case DATE:
                    currentStrValue = String.valueOf(internalRow.getInt(colIndex));
                    break;
                case DATETIME:
                    currentStrValue = String.valueOf(internalRow.getLong(colIndex));
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported column type: " + field.getType());
            }
            int hashCode = StarRocksWriterUtils.murmur3_32(currentStrValue);
            Long longSum = (Long) dqcResMap.getOrDefault(colName, 0L);
            dqcResMap.put(colName, longSum + hashCode);
        }
    }

    public SegmentLoadDqc merge(SegmentLoadDqc other) {
        rows += other.rows;
        for (Map.Entry<String, Object> entry : other.dqcResMap.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Float) {
                Float sum = (Float) dqcResMap.getOrDefault(colName, 0f);
                dqcResMap.put(colName, sum + (Float) value);
            } else if (value instanceof Double) {
                Double sum = (Double) dqcResMap.getOrDefault(colName, 0d);
                dqcResMap.put(colName, sum + (Double) value);
            } else {
                Long sum = (Long) dqcResMap.getOrDefault(colName, 0L);
                dqcResMap.put(colName, (Long) sum + (Long) value);
            }
        }
        return this;
    }

    public static String getDqcJsonRes(List<SegmentLoadDqc> dqcList) {
        SegmentLoadDqc allDqcRes = new SegmentLoadDqc();
        JSONArray tabletDqcJson = new JSONArray();
        for (SegmentLoadDqc dqc : dqcList) {
            allDqcRes.merge(dqc);
            tabletDqcJson.put(dqc.toJsonObject());
        }
        JSONObject res = allDqcRes.toJsonObject();
        res.put("tablet_dqc", tabletDqcJson);
        return res.toString();
    }
}
