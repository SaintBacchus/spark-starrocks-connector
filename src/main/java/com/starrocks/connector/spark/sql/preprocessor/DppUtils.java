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

import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlColumn;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
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

    public static Class getClassFromColumn(EtlColumn column) throws SparkWriteSDKException {
        switch (column.columnType) {
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
                return BigDecimal.valueOf(column.precision, column.scale).getClass();
            default:
                return String.class;
        }
    }

    public static DataType getDataTypeFromColumn(EtlJobConfig.EtlColumn column, boolean regardDistinctColumnAsBinary) {
        DataType dataType;
        switch (column.columnType) {
            case "BOOLEAN":
                dataType = DataTypes.StringType;
                break;
            case "TINYINT":
                dataType = DataTypes.ByteType;
                break;
            case "SMALLINT":
                dataType = DataTypes.ShortType;
                break;
            case "INT":
                dataType = DataTypes.IntegerType;
                break;
            case "DATETIME":
                dataType = DataTypes.TimestampType;
                break;
            case "BIGINT":
                dataType = DataTypes.LongType;
                break;
            case "LARGEINT":
                dataType = DataTypes.StringType;
                break;
            case "FLOAT":
                dataType = DataTypes.FloatType;
                break;
            case "DOUBLE":
                dataType = DataTypes.DoubleType;
                break;
            case "DATE":
                dataType = DataTypes.DateType;
                break;
            case "CHAR":
            case "VARCHAR":
            case "OBJECT":
            case "PERCENTILE":
            case "JSON":
                dataType = DataTypes.StringType;
                break;
            case "HLL":
            case "BITMAP":
                dataType = regardDistinctColumnAsBinary ? DataTypes.BinaryType : DataTypes.StringType;
                break;
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                dataType = DecimalType.apply(column.precision, column.scale);
                break;
            case "BINARY":
            case "VARBINARY":
                dataType = DataTypes.BinaryType;
                break;
            default:
                throw new RuntimeException("Unsupported column type:" + column.columnType);
        }
        return dataType;
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
            DataType structColumnType = getDataTypeFromColumn(column, regardDistinctColumnAsBinary);
            StructField field = DataTypes.createStructField(column.columnName, structColumnType, column.isAllowNull);
            fields.add(field);
        }
        return DataTypes.createStructType(fields);
    }

}