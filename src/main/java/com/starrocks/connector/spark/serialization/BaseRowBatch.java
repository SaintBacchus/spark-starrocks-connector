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

import com.starrocks.connector.spark.DataType;
import com.starrocks.connector.spark.exception.StarRocksException;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * row batch data container.
 */
public abstract class BaseRowBatch implements Iterator<List<Object>>, AutoCloseable {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final StarRocksSchema schema;
    protected final ZoneId timeZone;
    protected final Map<String, StarRocksField> fieldMap;

    protected List<Row> rowBatch = new ArrayList<>();
    private int rowOffset = 0;
    protected long rowCount = 0;

    public BaseRowBatch(StarRocksSchema schema) {
        this(schema, ZoneId.systemDefault());
    }

    public BaseRowBatch(StarRocksSchema schema, ZoneId timezone) {
        this.schema = schema;
        this.timeZone = timezone;
        this.fieldMap = schema.getColumns().stream()
                .collect(
                        Collectors.toMap(
                                StarRocksField::getName,
                                Function.identity()
                        )
                );
    }

    @Override
    public boolean hasNext() {
        return rowOffset < rowCount;
    }

    @Override
    public List<Object> next() throws StarRocksException {
        if (!hasNext()) {
            throw new NoSuchElementException(
                    "Row offset " + rowOffset + " is larger than row size " + rowCount);
        }
        return rowBatch.get(rowOffset++).getCols();
    }

    /**
     * Fill the row batch by arrow data.
     */
    protected final void fillRowBatchByArrowData(VectorSchemaRoot vsr) {
        int rowCountInBatch = vsr.getRowCount();
        int colSize = schema.getColumns().size();
        List<FieldVector> fieldVectors = vsr.getFieldVectors();
        if (rowCountInBatch <= 0 || fieldVectors.isEmpty()) {
            logger.warn("Ignore empty arrow data, rowCnt: {}, vectors: {}", rowCountInBatch, fieldVectors.size());
            return;
        }

        if (fieldVectors.size() != colSize) {
            throw new IllegalStateException(
                    "Column size '" + colSize + "' is not equal to arrow field size '" + fieldVectors.size() + "'"
            );
        }

        // init rows for the current batch data
        IntStream.range(0, rowCountInBatch).forEach(i -> rowBatch.add(new Row(colSize)));
        this.doFillRowBatchByArrowData(fieldVectors, rowCountInBatch, colSize);

        rowCount += rowCountInBatch;
    }

    protected abstract void doFillRowBatchByArrowData(List<FieldVector> vectors, int rowCountInBatch, int colSize);

    public long getRowCount() {
        return rowCount;
    }

    protected <F extends FieldVector, V> void fillColumnValues(F fieldVector,
                                                               int rowCountInBatch,
                                                               BiFunction<F, Integer, V> function) {
        for (int rowIdx = 0; rowIdx < rowCountInBatch; rowIdx++) {
            long idx = rowCount + rowIdx;
            if (idx < 0 || idx > Integer.MAX_VALUE) {
                throw new IllegalStateException("Invalid row idx " + idx + ", current row count is " + rowCount);
            }

            rowBatch.get((int) idx)
                    .put(fieldVector.isNull(rowIdx) ? null : function.apply(fieldVector, rowIdx));

        }
    }

    protected DataType resolveFieldType(List<FieldVector> fieldVectors, int colIdx) {
        FieldVector curFieldVector = fieldVectors.get(colIdx);

        StarRocksField vectorField = fieldMap.get(curFieldVector.getName());
        if (null != vectorField
                && null != vectorField.getType()
                && DataType.isSupported(vectorField.getType().getName())) {
            return DataType.of(vectorField.getType().getName());
        }

        StarRocksField schemaField = schema.getColumns().get(colIdx);
        if (null != schemaField
                && null != schemaField.getType()
                && DataType.isSupported(schemaField.getType().getName())) {
            logger.warn("Resolve field type[{}] from schema, idx: {}, vector[name: {}, type: {}]",
                    schemaField.getType(), colIdx, curFieldVector.getName(),
                    Optional.ofNullable(vectorField).map(StarRocksField::getType).orElse(null));
            return DataType.of(schemaField.getType().getName());
        }

        return DataType.fromArrowMinorType(curFieldVector.getMinorType());
    }

    protected static String typeMismatchMessage(final DataType columnType,
                                                final Types.MinorType arrowType) {
        return String.format(
                "Column type is %1$s, but mapped type is %2$s.",
                columnType, Optional.ofNullable(arrowType).map(Enum::name).orElse(null)
        );
    }

    public static class Row {

        private final List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }

    }

}
