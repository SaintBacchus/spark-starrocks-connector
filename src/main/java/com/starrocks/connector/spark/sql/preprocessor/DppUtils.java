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

package com.starrocks.connector.spark.sql.preprocessor;

import com.starrocks.connector.spark.util.DataTypeUtils;
import com.starrocks.format.rest.model.Column;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.CRC32;

public class DppUtils {

    public static final String BUCKET_ID = "__bucketId__";

    public static Class getClassFromColumn(Column.Type colType) throws SparkWriteSDKException {
        if (null == colType || null == colType.getName()) {
            throw new IllegalArgumentException("Null column data type.");
        }

        switch (StringUtils.upperCase(colType.getName())) {
            case "BOOLEAN":
                return Boolean.class;
            case "TINYINT":
            case "SMALLINT":
                return Short.class;
            case "INT":
                return Integer.class;
            case "DATETIME":
                return java.sql.Timestamp.class;
            case "BIGINT":
                return Long.class;
            case "LARGEINT":
                throw new SparkWriteSDKException("LARGEINT is not supported now");
            case "FLOAT":
                return Float.class;
            case "DOUBLE":
                return Double.class;
            case "DATE":
                return Date.class;
            case "HLL":
            case "CHAR":
            case "VARCHAR":
            case "BITMAP":
            case "OBJECT":
            case "PERCENTILE":
            case "JSON":
                return String.class;
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                return BigDecimal.valueOf(colType.getPrecision(), colType.getScale()).getClass();
            default:
                throw new UnsupportedOperationException("Unsupported column type: " + colType.getName());
        }
    }

    public static ByteBuffer getHashValue(Object o, DataType type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // null as int 0
        if (o == null) {
            buffer.putInt(0);
            buffer.flip();
            return buffer;
        }

        // varchar and char
        if (type.equals(DataTypes.StringType)) {
            try {
                String str = String.valueOf(o);
                return ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (type.equals(DataTypes.BinaryType)) {
            return ByteBuffer.wrap((byte[]) o);
        }

        // TODO(wyb): Support decimal date datetime
        if (type.equals(DataTypes.BooleanType)) {
            byte b = (boolean) o ? (byte) 1 : (byte) 0;
            buffer.put(b);
        } else if (type.equals(DataTypes.ByteType)) {
            buffer.put((byte) o);
        } else if (type.equals(DataTypes.ShortType)) {
            buffer.putShort((Short) o);
        } else if (type.equals(DataTypes.IntegerType)) {
            buffer.putInt((Integer) o);
        } else if (type.equals(DataTypes.LongType)) {
            buffer.putLong((Long) o);
        }
        buffer.flip();
        return buffer;
    }

    public static long getHashValue(InternalRow row, List<String> distributeColumns, StructType dstTableSchema) {
        CRC32 hashValue = new CRC32();
        for (String distColumn : distributeColumns) {
            Object columnObject = row.get((int) dstTableSchema.getFieldIndex(distColumn).get(),
                    dstTableSchema.apply(distColumn).dataType());
            ByteBuffer buffer = getHashValue(columnObject, dstTableSchema.apply(distColumn).dataType());
            hashValue.update(buffer.array(), 0, buffer.limit());
        }
        return hashValue.getValue();
    }

    public static StructType createDstTableSchema(List<EtlJobConfig.EtlColumn> columns,
                                                  boolean addBucketIdColumn,
                                                  boolean regardDistinctColumnAsBinary) {
        List<StructField> fields = new ArrayList<>();
        if (addBucketIdColumn) {
            StructField bucketIdField = DataTypes.createStructField(BUCKET_ID, DataTypes.StringType, true);
            fields.add(bucketIdField);
        }
        for (EtlJobConfig.EtlColumn column : columns) {
            DataType structColumnType = DataTypeUtils.toSparkDataType(column.getColumnType(), regardDistinctColumnAsBinary);
            StructField field = DataTypes.createStructField(column.getColumnName(), structColumnType, column.isAllowNull());
            fields.add(field);
        }
        return DataTypes.createStructType(fields);
    }

}