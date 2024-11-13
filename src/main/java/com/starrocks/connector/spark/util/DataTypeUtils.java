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

import com.starrocks.format.rest.model.Column;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.util.stream.Collectors;

public class DataTypeUtils {

    private DataTypeUtils() {
    }

    public static DataType toSparkDataType(Column.Type type) {
        if (null == type || null == type.getName()) {
            throw new IllegalArgumentException("Null column data type.");
        }

        switch (StringUtils.upperCase(type.getName())) {
            case "BOOLEAN":
                return DataTypes.BooleanType;
            case "TINYINT":
                // mysql does not have boolean type, and starrocks `information_schema`.`COLUMNS` will return
                // a "tinyint" data type for both StarRocks BOOLEAN and TINYINT type, We distinguish them by
                // column size, and the size of BOOLEAN is null
                return type.getColumnSize() == null ? DataTypes.BooleanType : DataTypes.ByteType;
            case "SMALLINT":
                return DataTypes.ShortType;
            case "INT":
                return DataTypes.IntegerType;
            case "BIGINT":
                return DataTypes.LongType;
            case "FLOAT":
                return DataTypes.FloatType;
            case "DOUBLE":
                return DataTypes.DoubleType;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                return DataTypes.createDecimalType(type.getPrecision(), type.getScale());
            case "BIGINT UNSIGNED":
            case "LARGEINT":
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "JSON":
                return DataTypes.StringType;
            case "DATE":
                return DataTypes.DateType;
            case "DATETIME":
                return DataTypes.TimestampType;
            case "BINARY":
            case "VARBINARY":
            case "BITMAP":
            case "HLL":
                return DataTypes.BinaryType;
            case "ARRAY":
                return DataTypes.createArrayType(toSparkDataType(type.getItemType()));
            case "STRUCT":
                return DataTypes.createStructType(
                        type.getFields().stream()
                                .map(field -> new StructField(
                                        field.getName(),
                                        toSparkDataType(field.getType()),
                                        Boolean.TRUE.equals(field.getAllowNull()),
                                        Metadata.empty()
                                )).collect(Collectors.toList())
                );
            case "MAP":
                return DataTypes.createMapType(
                        toSparkDataType(type.getKeyType()),
                        toSparkDataType(type.getValueType())
                );
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported column data type %s", type.getName())
                );
        }
    }

    public static DataType toSparkDataType(Column.Type type, boolean regardDistinctColumnAsBinary) {
        if (null == type || null == type.getName()) {
            throw new IllegalArgumentException("Null column data type.");
        }

        switch (StringUtils.upperCase(type.getName())) {
            case "BOOLEAN":
                return DataTypes.StringType;
            case "TINYINT":
                return DataTypes.ByteType;
            case "SMALLINT":
                return DataTypes.ShortType;
            case "INT":
                return DataTypes.IntegerType;
            case "DATETIME":
                return DataTypes.TimestampType;
            case "BIGINT":
                return DataTypes.LongType;
            case "FLOAT":
                return DataTypes.FloatType;
            case "DOUBLE":
                return DataTypes.DoubleType;
            case "DATE":
                return DataTypes.DateType;
            case "LARGEINT":
            case "CHAR":
            case "VARCHAR":
            case "OBJECT":
            case "PERCENTILE":
            case "JSON":
                return DataTypes.StringType;
            case "HLL":
            case "BITMAP":
                return regardDistinctColumnAsBinary ? DataTypes.BinaryType : DataTypes.StringType;
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                return DecimalType.apply(type.getPrecision(), type.getScale());
            case "BINARY":
            case "VARBINARY":
                return DataTypes.BinaryType;
            case "ARRAY":
                return DataTypes.createArrayType(
                        toSparkDataType(type.getItemType(), regardDistinctColumnAsBinary)
                );
            case "STRUCT":
                return DataTypes.createStructType(
                        type.getFields().stream()
                                .map(field -> new StructField(
                                        field.getName(),
                                        toSparkDataType(field.getType(), regardDistinctColumnAsBinary),
                                        Boolean.TRUE.equals(field.getAllowNull()),
                                        Metadata.empty()
                                )).collect(Collectors.toList())
                );
            case "MAP":
                return DataTypes.createMapType(
                        toSparkDataType(type.getKeyType(), regardDistinctColumnAsBinary),
                        toSparkDataType(type.getValueType(), regardDistinctColumnAsBinary)
                );
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported column data type %s", type.getName())
                );
        }
    }

}