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

import com.starrocks.connector.spark.DataType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;

// Parser to validate value for different type
public abstract class ColumnParser implements Serializable {

    protected static final Logger LOG = LogManager.getLogger(ColumnParser.class);

    // thread safe formatter
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static ColumnParser create(EtlJobConfig.EtlColumn column) throws SparkWriteSDKException {
        DataType colType = DataType.of(column.getColumnType().getName());
        switch (colType) {
            case BOOLEAN:
                return new BooleanParser();
            case TINYINT:
                return new TinyIntParser();
            case SMALLINT:
                return new SmallIntParser();
            case INT:
                return new IntParser();
            case BIGINT:
                return new BigIntParser();
            case LARGEINT:
                return new LargeIntParser();
            case FLOAT:
                return new FloatParser();
            case DOUBLE:
                return new DoubleParser();
            case DATE:
                return new DateParser();
            case DATETIME:
                return new DatetimeParser();
            case CHAR:
            case VARCHAR:
            case JSON:
            case BITMAP:
            case HLL:
                return new StringParser(column);
            case BINARY:
            case VARBINARY:
                return new BinaryParser(column);
            case DECIMAL:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new DecimalParser(column);
            case ARRAY:
            case STRUCT:
            case MAP:
                return new NoopParser();
            default:
                throw new SparkWriteSDKException("Unsupported type: " + colType);
        }
    }

    public abstract boolean parse(String value);
}

class TinyIntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Byte.parseByte(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class SmallIntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Short.parseShort(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class IntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class BigIntParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Long.parseLong(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}

class FloatParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Float ret = Float.parseFloat(value);
            return !ret.isNaN() && !ret.isInfinite();
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

class DoubleParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            Double ret = Double.parseDouble(value);
            return !ret.isInfinite() && !ret.isNaN();
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

class BooleanParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        if (value.equalsIgnoreCase("true")
                || value.equalsIgnoreCase("false")
                || value.equals("0") || value.equals("1")) {
            return true;
        }
        return false;
    }
}

class DateParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            DATE_FORMATTER.parse(value);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }
}

class DatetimeParser extends ColumnParser {
    @Override
    public boolean parse(String value) {
        try {
            DATE_TIME_FORMATTER.parse(value);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }
}

class StringParser extends ColumnParser {

    private EtlJobConfig.EtlColumn etlColumn;

    public StringParser(EtlJobConfig.EtlColumn etlColumn) {
        this.etlColumn = etlColumn;
    }

    @Override
    public boolean parse(String value) {
        try {
            return value.getBytes(StandardCharsets.UTF_8).length <= etlColumn.getColumnType().getColumnSize();
        } catch (Exception e) {
            throw new RuntimeException("string check failed ", e);
        }
    }
}

class BinaryParser extends ColumnParser {

    private EtlJobConfig.EtlColumn etlColumn;

    public BinaryParser(EtlJobConfig.EtlColumn etlColumn) {
        this.etlColumn = etlColumn;
    }

    @Override
    public boolean parse(String value) {
        return true;
    }
}

class DecimalParser extends ColumnParser {

    public static int PRECISION = 27;
    public static int SCALE = 9;

    private BigDecimal maxValue;
    private BigDecimal minValue;

    public DecimalParser(EtlJobConfig.EtlColumn etlColumn) {
        StringBuilder precisionStr = new StringBuilder();

        int precision = etlColumn.getColumnType().getPrecision();
        int scale = etlColumn.getColumnType().getScale();
        for (int i = 0; i < precision - scale; i++) {
            precisionStr.append("9");
        }
        StringBuilder scaleStr = new StringBuilder();
        for (int i = 0; i < scale; i++) {
            scaleStr.append("9");
        }
        maxValue = new BigDecimal(precisionStr + "." + scaleStr);
        minValue = new BigDecimal("-" + precisionStr + "." + scaleStr);
    }

    @Override
    public boolean parse(String value) {
        try {
            BigDecimal bigDecimal = new BigDecimal(value);
            return bigDecimal.precision() - bigDecimal.scale() <= PRECISION - SCALE && bigDecimal.scale() <= SCALE;
        } catch (NumberFormatException e) {
            return false;
        } catch (Exception e) {
            throw new RuntimeException("decimal parse failed ", e);
        }
    }

    public BigDecimal getMaxValue() {
        return maxValue;
    }

    public BigDecimal getMinValue() {
        return minValue;
    }
}

class LargeIntParser extends ColumnParser {

    private BigInteger maxValue = new BigInteger("170141183460469231731687303715884105727");
    private BigInteger minValue = new BigInteger("-170141183460469231731687303715884105728");

    @Override
    public boolean parse(String value) {
        try {
            BigInteger inputValue = new BigInteger(value);
            return inputValue.compareTo(maxValue) < 0 && inputValue.compareTo(minValue) > 0;
        } catch (NumberFormatException e) {
            return false;
        } catch (ArithmeticException e) {
            LOG.warn("int value is too big even for java BigInteger,value={}" + value);
            return false;
        } catch (Exception e) {
            throw new RuntimeException("large int parse failed:" + value, e);
        }
    }
}

class NoopParser extends ColumnParser {

    @Override
    public boolean parse(String value) {
        return true;
    }

}