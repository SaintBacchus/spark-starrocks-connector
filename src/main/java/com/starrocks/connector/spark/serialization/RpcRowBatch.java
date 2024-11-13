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
import com.starrocks.thrift.TScanBatchResult;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.spark.sql.types.Decimal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * row batch data container.
 */
public class RpcRowBatch extends BaseRowBatch {

    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter dateTimeUsFormatter;
    private DateTimeFormatter dateFormatter;

    private final ArrowStreamReader arrowStreamReader;
    private final RootAllocator rootAllocator;

    public RpcRowBatch(TScanBatchResult nextResult, StarRocksSchema schema) {
        this(nextResult, schema, ZoneId.systemDefault());
    }

    public RpcRowBatch(TScanBatchResult nextResult,
                       StarRocksSchema schema,
                       ZoneId timeZone) {
        super(schema, timeZone);

        this.dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(timeZone);
        this.dateTimeUsFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(timeZone);
        this.dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(timeZone);

        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        try {
            VectorSchemaRoot root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                this.fillRowBatchByArrowData(root);
            }
        } catch (Exception e) {
            logger.error("Read starrocks data failed, {}", e.getMessage(), e);
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
                case FLOAT:
                    Preconditions.checkArgument(
                            MinorType.FLOAT4.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((Float4Vector) curVector, rowCountInBatch, Float4Vector::get);
                    break;
                case TIME:
                case DOUBLE:
                    Preconditions.checkArgument(
                            MinorType.FLOAT8.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues((Float8Vector) curVector, rowCountInBatch, Float8Vector::get);
                    break;
                case DATE:
                    Preconditions.checkArgument(
                            MinorType.VARCHAR.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues(
                            (VarCharVector) curVector,
                            rowCountInBatch,
                            (vec, idx) -> Date.valueOf(
                                    LocalDate.parse(new String(vec.get(idx), StandardCharsets.UTF_8), dateFormatter)
                            )
                    );
                    break;
                case DATETIME:
                    Preconditions.checkArgument(
                            MinorType.VARCHAR.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues(
                            (VarCharVector) curVector,
                            rowCountInBatch,
                            (vec, idx) -> {
                                String value = new String(vec.get(idx), StandardCharsets.UTF_8);
                                try {
                                    return Timestamp.valueOf(LocalDateTime.parse(value, dateTimeFormatter));
                                } catch (DateTimeParseException e) {
                                    return Timestamp.valueOf(LocalDateTime.parse(value, dateTimeUsFormatter));
                                }
                            }
                    );
                    break;
                case LARGEINT:
                case CHAR:
                case VARCHAR:
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
                    Preconditions.checkArgument(
                            MinorType.VARCHAR.equals(arrowMinorType),
                            typeMismatchMessage(columnType, arrowMinorType)
                    );
                    fillColumnValues(
                            (VarCharVector) curVector,
                            rowCountInBatch,
                            (vec, idx) -> {
                                String value = new String(vec.get(idx), StandardCharsets.UTF_8);
                                try {
                                    return Decimal.apply(new BigDecimal(value));
                                } catch (NumberFormatException e) {
                                    throw new StarRocksException("Decimal response result '" + value + "' is illegal.");
                                }
                            }
                    );
                    break;
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
                    throw new IllegalStateException("Unsupported data type " + columnType);
            }
        }
    }

    @Override
    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }
}
