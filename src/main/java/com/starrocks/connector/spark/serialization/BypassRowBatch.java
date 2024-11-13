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

package com.starrocks.connector.spark.serialization;

import com.google.common.base.Preconditions;
import com.starrocks.connector.spark.DataType;
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.spark.sql.types.Decimal;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static com.starrocks.connector.spark.DataType.MILLIS_OF_DAY;
import static java.util.Objects.requireNonNull;

/**
 * row batch data container.
 */
public class BypassRowBatch extends BaseRowBatch {

    private final VectorSchemaRoot vsr;

    public BypassRowBatch(VectorSchemaRoot vsr,
                          StarRocksSchema schema,
                          ZoneId timezone) {
        super(schema, timezone);
        this.vsr = requireNonNull(vsr, "Arrow vector schema root is null");
        try {
            this.fillRowBatchByArrowData(vsr);
        } catch (Exception e) {
            logger.error("Read starrocks data in bypass mode error, {}", e.getMessage(), e);
            if (e instanceof StarRocksException) {
                throw (StarRocksException) e;
            }
            throw new StarRocksException(e.getMessage());
        } finally {
            close();
        }
    }

    @Override
    protected void doFillRowBatchByArrowData(List<FieldVector> vectors, int rowCountInBatch, int colSize) {
        for (int colIdx = 0; colIdx < colSize; colIdx++) {
            FieldVector curVector = vectors.get(colIdx);
            MinorType arrowMinorType = curVector.getMinorType();
            DataType columnType = resolveFieldType(vectors, colIdx);
            switch (columnType) {
                case NULL:
                    fillColumnValues(curVector, rowCountInBatch, (fieldVector, idx) -> null);
                    break;
                case BOOLEAN:
                    Preconditions.checkArgument(
                            MinorType.BIT.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((BitVector) curVector, rowCountInBatch, (vec, idx) -> vec.get(idx) != 0);
                    break;
                case TINYINT:
                    Preconditions.checkArgument(
                            MinorType.TINYINT.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((TinyIntVector) curVector, rowCountInBatch, TinyIntVector::get);
                    break;
                case SMALLINT:
                    Preconditions.checkArgument(
                            MinorType.SMALLINT.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((SmallIntVector) curVector, rowCountInBatch, SmallIntVector::get);
                    break;
                case INT:
                    Preconditions.checkArgument(
                            MinorType.INT.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((IntVector) curVector, rowCountInBatch, IntVector::get);
                    break;
                case BIGINT:
                    Preconditions.checkArgument(
                            MinorType.BIGINT.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((BigIntVector) curVector, rowCountInBatch, BigIntVector::get);
                    break;
                case LARGEINT:
                    Preconditions.checkArgument(
                            MinorType.DECIMAL256.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues(
                            (Decimal256Vector) curVector,
                            rowCountInBatch,
                            (vec, idx) -> vec.getObject(idx).toPlainString()
                    );
                    break;
                case FLOAT:
                    Preconditions.checkArgument(
                            MinorType.FLOAT4.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((Float4Vector) curVector, rowCountInBatch, Float4Vector::get);
                    break;
                case DOUBLE:
                    Preconditions.checkArgument(
                            MinorType.FLOAT8.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((Float8Vector) curVector, rowCountInBatch, Float8Vector::get);
                    break;
                case DATE:
                    if (curVector instanceof DateDayVector) {
                        Preconditions.checkArgument(
                                MinorType.DATEDAY.equals(arrowMinorType),
                                typeMismatchMessage(columnType, arrowMinorType)
                        );
                        fillColumnValues(
                                (DateDayVector) curVector,
                                rowCountInBatch,
                                (vec, idx) -> Date.valueOf(LocalDate.ofEpochDay(vec.get(idx)))
                        );
                    } else if (curVector instanceof DateMilliVector) {
                        Preconditions.checkArgument(
                                MinorType.DATEMILLI.equals(arrowMinorType),
                                typeMismatchMessage(columnType, arrowMinorType)
                        );
                        fillColumnValues(
                                (DateMilliVector) curVector,
                                rowCountInBatch,
                                (vec, idx) -> Date.valueOf(LocalDate.ofEpochDay(vec.get(idx) / MILLIS_OF_DAY))
                        );
                    } else {
                        throw new IllegalStateException(
                                "Unsupported field vector '" + curVector.getClass().getCanonicalName() + "' for date type."
                        );
                    }
                    break;
                case DATETIME:
                    Preconditions.checkArgument(
                            MinorType.TIMESTAMPMILLI.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues(
                            (TimeStampMilliVector) curVector,
                            rowCountInBatch,
                            (vec, idx) -> Timestamp.valueOf(vec.getObject(idx))
                    );
                    break;
                case CHAR:
                case VARCHAR:
                case JSON:
                    Preconditions.checkArgument(
                            MinorType.VARCHAR.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues(
                            (VarCharVector) curVector,
                            rowCountInBatch,
                            (vec, idx) -> new String(vec.get(idx), StandardCharsets.UTF_8)
                    );
                    break;
                case BINARY:
                case VARBINARY:
                    Preconditions.checkArgument(
                            MinorType.VARBINARY.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((VarBinaryVector) curVector, rowCountInBatch, VarBinaryVector::get);
                    break;
                case DECIMAL:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    Preconditions.checkArgument(
                            MinorType.DECIMAL.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues(
                            (DecimalVector) curVector,
                            rowCountInBatch,
                            (vec, idx) -> Decimal.apply(vec.getObject(idx))
                    );
                    break;
                default:
                    throw new StarRocksException("Unsupported data type when do bypass read: " + columnType);
            }
        }
    }

    @Override
    public void close() {
        if (null != vsr) {
            vsr.close();
        }
    }
}
