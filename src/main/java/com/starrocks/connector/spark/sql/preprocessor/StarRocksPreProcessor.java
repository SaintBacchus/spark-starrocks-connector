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

import com.starrocks.connector.spark.sql.schema.TableModel;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

// This class is a Spark-based data preprocessing program,
// which will make use of the distributed compute framework of spark to
// do ETL job/sort/preaggregate jobs in spark job
// to boost the process of large amount of data load.
// the process steps are as following:
// 1. load data
//     1.1 load data from path/hive table
//     1.2 do the etl process
// 2. repartition data by using starrocks data model(partition and bucket)
// 3. process aggregation if needed
// 4. write data to parquet file
public final class StarRocksPreProcessor implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksPreProcessor.class);

    // save the hadoop configuration from spark session.
    // because hadoop configuration is not serializable,
    // we need to wrap it so that we can use it in executor.
    private SerializableConfiguration serializableHadoopConf;

    // just for ut
    public StarRocksPreProcessor(Configuration hadoopConf) {
        serializableHadoopConf = new SerializableConfiguration(hadoopConf);
    }

    // write data to parquet file by using writing the parquet scheme of spark.
    public JavaPairRDD<List<Object>, Object[]> repartitionAndSortedRDD(JavaPairRDD<List<Object>, Object[]> resultRDD,
                                                                       Map<String, Integer> bucketKeyMap) {
        // TODO(wb) should deal largeint as BigInteger instead of string when using biginteger as key,
        // data type may affect sorting logic
        return resultRDD.repartitionAndSortWithinPartitions(new BucketPartitioner(bucketKeyMap),
                new BucketComparator());
    }

    public JavaRDD<InternalRow> processPartition(JavaPairRDD<List<Object>, Object[]> rdd,
                                                 EtlJobConfig.EtlIndex indexMeta,
                                                 SparkRDDAggregator[] sparkRDDAggregators,
                                                 Map<Long, EtlJobConfig.EtlPartition> partitionKeyMap,
                                                 boolean isDuplicateTable) {
        StructType tableSchema = DppUtils.createDstTableSchema(indexMeta.getColumns(), false, true);

        // add tablet column
        StructType internalTableSchema = tableSchema.add("tablet_id", LongType);
        ExpressionEncoder<Row> encoder = ExpressionEncoder.apply(RowEncoder.encoderFor(internalTableSchema));
        ExpressionEncoder.Serializer<Row> toRow = encoder.createSerializer();

        // TODO(will have > 2 buckets per Partition ?? )
        return rdd.mapPartitions((Iterator<Tuple2<List<Object>, Object[]>> iter) ->
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false)
                        .map(record -> {
                            List<Object> keyColumns = record._1;
                            Object[] valueColumns = record._2;
                            String curBucketKey = keyColumns.get(0).toString();
                            List<Object> allColumnPerRow = new ArrayList<>(keyColumns.subList(1, keyColumns.size()));
                            // key first element is partition_bucket
                            if (isDuplicateTable) {
                                Collections.addAll(allColumnPerRow, valueColumns);
                            } else {
                                for (int i = 0; i < valueColumns.length; ++i) {
                                    allColumnPerRow.add(sparkRDDAggregators[i].finish(valueColumns[i]));
                                }
                            }

                            // flush current writer and create a new writer
                            String[] bucketKey = curBucketKey.split("_");
                            if (bucketKey.length != 2) {
                                throw new IllegalStateException("Invalid bucket key: " + curBucketKey);
                            }

                            Long partitionId = Long.parseLong(bucketKey[0]);
                            int bucketId = Integer.parseInt(bucketKey[1]);
                            // must order by asc
                            Long tabletId = partitionKeyMap.get(partitionId).getTabletIds().get(bucketId);
                            allColumnPerRow.add(tabletId);

                            Row rowWithTabletId = RowFactory.create(allColumnPerRow.toArray());
                            return toRow.apply(rowWithTabletId);
                        }).iterator());
    }

    // TODO(wb) one shuffle to calculate the rollup in the same level
    public JavaRDD<InternalRow> processRollupTree(RollupTreeNode rootNode,
                                                  JavaPairRDD<List<Object>, Object[]> rootRDD,
                                                  EtlJobConfig.EtlIndex baseIndex,
                                                  Map<String, Integer> bucketKeyMap,
                                                  Map<Long, EtlJobConfig.EtlPartition> partitionKeyMap)
            throws SparkWriteSDKException {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Long, JavaPairRDD<List<Object>, Object[]>> parentRDDMap = new HashMap<>();
        parentRDDMap.put(baseIndex.getIndexId(), rootRDD);
        Map<Long, JavaPairRDD<List<Object>, Object[]>> childrenRDDMap = new HashMap<>();
        while (!nodeQueue.isEmpty()) {
            RollupTreeNode curNode = nodeQueue.poll();
            LOG.info("Start to process index: {}", curNode.indexId);
            if (curNode.children != null) {
                for (RollupTreeNode child : curNode.children) {
                    nodeQueue.offer(child);
                }
            }
            JavaPairRDD<List<Object>, Object[]> curRDD;
            // column select for rollup
            if (curNode.level != currentLevel) {
                for (JavaPairRDD<List<Object>, Object[]> rdd : parentRDDMap.values()) {
                    rdd.unpersist();
                }
                currentLevel = curNode.level;
                parentRDDMap.clear();
                parentRDDMap = childrenRDDMap;
                childrenRDDMap = new HashMap<>();
            }

            long parentIndexId = baseIndex.getIndexId();
            if (curNode.parent != null) {
                parentIndexId = curNode.parent.indexId;
            }

            JavaPairRDD<List<Object>, Object[]> parentRDD = parentRDDMap.get(parentIndexId);

            // aggregate
            SparkRDDAggregator[] rddAggregators = new SparkRDDAggregator[curNode.valueColumnNames.size()];
            if (TableModel.notValidType(curNode.indexMeta.getIndexType())) {
                throw new IllegalStateException("Invalid index type: " + curNode.indexMeta.getIndexType());
            }
            final boolean isDuplicateTable = TableModel.DUPLICATE_KEY.is(curNode.indexMeta.getIndexType());
            curRDD = processRDDAggregate(parentRDD, curNode, rddAggregators, isDuplicateTable);

            childrenRDDMap.put(curNode.indexId, curRDD);

            if (curNode.children != null && curNode.children.size() > 1) {
                // if the children number larger than 1, persist the dataframe for performance
                curRDD.persist(StorageLevel.MEMORY_AND_DISK());
            }
            JavaPairRDD<List<Object>, Object[]> pairRDD = repartitionAndSortedRDD(curRDD, bucketKeyMap);
            return processPartition(pairRDD, baseIndex, rddAggregators, partitionKeyMap, isDuplicateTable);
        }
        return new JavaSparkContext().emptyRDD();
    }

    private JavaPairRDD<List<Object>, Object[]> processRDDAggregate(JavaPairRDD<List<Object>, Object[]> currentPairRDD,
                                                                    RollupTreeNode curNode,
                                                                    SparkRDDAggregator[] rddAggregators,
                                                                    boolean isDuplicateTable)
            throws SparkWriteSDKException {

        if (isDuplicateTable) {
            int idx = 0;
            for (int i = 0; i < curNode.indexMeta.getColumns().size(); i++) {
                if (!curNode.indexMeta.getColumns().get(i).isKey()) {
                    // duplicate table doesn't need aggregator
                    // init a aggregator here just for keeping interface compatibility when writing data to HDFS
                    rddAggregators[idx] = new DefaultSparkRDDAggregator();
                    idx++;
                }
            }

            return curNode.indexMeta.isBaseIndex() ? currentPairRDD :
                    currentPairRDD.mapToPair(new EncodeRollupAggregateTableFunction(
                            getColumnIndexInParentRollup(
                                    curNode.keyColumnNames,
                                    curNode.valueColumnNames,
                                    curNode.parent.keyColumnNames,
                                    curNode.parent.valueColumnNames)));
        }

        int idx = 0;
        for (int i = 0; i < curNode.indexMeta.getColumns().size(); i++) {
            if (!curNode.indexMeta.getColumns().get(i).isKey()) {
                rddAggregators[idx] = SparkRDDAggregator.buildAggregator(curNode.indexMeta.getColumns().get(i));
                idx++;
            }
        }

        // TODO(wb) set the reduce concurrency by statistic instead of hList<String> distributeColumns = partitionInfo.getDistributionColumnRefs();ard code 200
        int aggregateConcurrency = 200;
        if (curNode.indexMeta.isBaseIndex()) {
            return currentPairRDD
                    .mapToPair(new EncodeBaseAggregateTableFunction(rddAggregators))
                    .reduceByKey(new AggregateReduceFunction(rddAggregators), aggregateConcurrency);
        } else {
            return currentPairRDD
                    .mapToPair(new EncodeRollupAggregateTableFunction(getColumnIndexInParentRollup(
                            curNode.keyColumnNames, curNode.valueColumnNames,
                            curNode.parent.keyColumnNames,
                            curNode.parent.valueColumnNames)))
                    .reduceByKey(new AggregateReduceFunction(rddAggregators), aggregateConcurrency);
        }
    }

    // get column index map from parent rollup to child rollup
    // not consider bucketId here
    private Pair<Integer[], Integer[]> getColumnIndexInParentRollup(List<String> childRollupKeyColumns,
                                                                    List<String> childRollupValueColumns,
                                                                    List<String> parentRollupKeyColumns,
                                                                    List<String> parentRollupValueColumns)
            throws SparkWriteSDKException {
        List<String> parentRollupColumns = new ArrayList<>();
        parentRollupColumns.addAll(parentRollupKeyColumns);
        parentRollupColumns.addAll(parentRollupValueColumns);

        List<Integer> keyMap = getChildColumnIds(childRollupKeyColumns, parentRollupColumns);
        List<Integer> valueMap = getChildColumnIds(childRollupValueColumns, parentRollupColumns);

        if (keyMap.size() != childRollupKeyColumns.size() || valueMap.size() != childRollupValueColumns.size()) {
            throw new SparkWriteSDKException(String.format(
                    "column map index from child to parent has error, key size src: %s, dst: %s; value size src: %s, dst: %s",
                    childRollupKeyColumns.size(), keyMap.size(), childRollupValueColumns.size(), valueMap.size()));
        }

        return Pair.of(keyMap.toArray(new Integer[keyMap.size()]), valueMap.toArray(new Integer[valueMap.size()]));
    }

    private List<Integer> getChildColumnIds(List<String> childRollupColumns, List<String> parentRollupColumns) {
        List<Integer> childColumnIds = new ArrayList<>();

        for (int i = 0; i < childRollupColumns.size(); i++) {
            for (int j = 0; j < parentRollupColumns.size(); j++) {
                if (StringUtils.equalsIgnoreCase(childRollupColumns.get(i), parentRollupColumns.get(j))) {
                    childColumnIds.add(j);
                    break;
                }
            }
        }

        return childColumnIds;
    }

    /**
     * check decimal,char/varchar
     */
    public boolean validateData(Object srcValue,
                                EtlJobConfig.EtlColumn etlColumn,
                                ColumnParser columnParser,
                                InternalRow row) {

        switch (etlColumn.getColumnType().getName()) {
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                DecimalParser decimalParser = (DecimalParser) columnParser;
                // TODO(wb):  support decimal round; see be DecimalV2Value::round
                if (srcValue != null) {
                    BigDecimal srcDecimal = ((Decimal) srcValue).toBigDecimal().bigDecimal();
                    if (decimalParser.getMaxValue().compareTo(srcDecimal) < 0 ||
                            decimalParser.getMinValue().compareTo(srcDecimal) > 0) {

                        LOG.warn(String.format(
                                "decimal value is not valid for defination, column=%s, value=%s,precision=%s,scale=%s",
                                etlColumn.getColumnName(), srcValue, srcDecimal.precision(),
                                srcDecimal.scale()));
                        return false;
                    }
                }
                break;
            case "CHAR":
            case "VARCHAR":
                // TODO(wb) padding char type
                int strSize = 0;
                if (srcValue != null &&
                        (strSize = srcValue.toString().getBytes(StandardCharsets.UTF_8).length) >
                                etlColumn.getColumnType().getColumnSize()) {
                    LOG.warn(String.format(
                            "the length of input is too long than schema. column_name:%s," +
                                    "input_str[%s],schema length:%s,actual length:%s",
                            etlColumn.getColumnName(), row.toString(), etlColumn.getColumnType().getColumnSize(), strSize));
                    return false;
                }
                break;
        }
        return true;
    }

    /**
     * 1 project column and reorder column
     * 2 validate data
     * 3 fill tuple with partition column
     */
    public JavaPairRDD<List<Object>, Object[]> fillTupleWithPartitionColumn(
            EtlJobConfig.EtlPartitionInfo partitionInfo,
            RDD<InternalRow> rdd,
            List<Integer> partitionKeyIndex,
            List<StarRocksRangePartitioner.PartitionRangeKey> partitionRangeKeys,
            List<StarRocksListPartitioner.PartitionListKey> partitionListKeys,
            List<String> keyColumnNames,
            List<String> valueColumnNames,
            StructType dstTableSchema,
            EtlJobConfig.EtlIndex baseIndex) throws SparkWriteSDKException {
        List<String> distributionColumnRefs = partitionInfo.getDistributionColumnRefs();

        PartitionType partitionType = PartitionType.elegantOf(partitionInfo.getPartitionType())
                .orElseThrow(() -> new UnsupportedOperationException(
                        "Unsupported partition type: " + partitionInfo.getPartitionType()));

        Partitioner partitioner;
        switch (partitionType) {
            case UNPARTITIONED:
            case RANGE:
                partitioner = new StarRocksRangePartitioner(
                        partitionInfo, partitionKeyIndex, partitionRangeKeys);
                break;
            case LIST:
                partitioner = new StarRocksListPartitioner(
                        partitionInfo, partitionKeyIndex, partitionListKeys);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported partition type: " + partitionType);
        }

        List<ColumnParser> parsers = new ArrayList<>();
        for (EtlJobConfig.EtlColumn column : baseIndex.getColumns()) {
            parsers.add(ColumnParser.create(column));
        }

        Set<Integer> partitionIdx = new HashSet<>(partitionKeyIndex);

        // use PairFlatMapFunction instead of PairMapFunction because there will be
        // 0 or 1 output row for 1 input row
        // TODO (jkj check for flatMapToPair change to mapToPair)
        JavaPairRDD<List<Object>, Object[]> resultPairRDD = rdd.toJavaRDD().mapToPair(row -> {
            Tuple2<List<Object>, Object[]> result = new Tuple2<>(null, null);

            RowContext rowContext = new RowContext();
            boolean validData = rowContext.processRow(
                    keyColumnNames, row, dstTableSchema, baseIndex, parsers,
                    true, keyColumnNames.size(), false, partitionIdx)
                    &&
                    rowContext.processRow(
                            valueColumnNames, row, dstTableSchema, baseIndex, parsers,
                            false, keyColumnNames.size(), false, partitionIdx);
            if (!validData) {
                return result;
            }

            int pid = partitioner.getPartition(new DppColumns(rowContext.getAllColumns()));
            if (pid < 0) {
                LOG.warn("Invalid partition for row: {}, abnormal rows num: {}", row, pid);
                return result;
            }

            // TODO(wb) support large int for hash
            long hashValue = DppUtils.getHashValue(row, distributionColumnRefs, dstTableSchema);
            int bucketId = (int) ((hashValue & 0xffffffff) % partitionInfo.getPartitions().get(pid).getBucketNum());
            long partitionId = partitionInfo.getPartitions().get(pid).getPartitionId();
            // bucketKey is partitionId_bucketId
            String bucketKey = partitionId + "_" + bucketId;

            List<Object> tuple = new ArrayList<>();
            tuple.add(bucketKey);
            tuple.addAll(rowContext.getKeyColumns());
            return new Tuple2<>(tuple, rowContext.getValColumns().toArray());
        });

        return resultPairRDD;
    }

    public class RowContext {

        private final List<Object> allColumns = new ArrayList<>();
        private final List<Object> keyColumns = new ArrayList<>();
        private final List<Object> valColumns = new ArrayList<>();

        public RowContext() {
        }

        public boolean processRow(List<String> columnNames,
                                  InternalRow row,
                                  StructType tableSchema,
                                  EtlJobConfig.EtlIndex baseIndex,
                                  List<ColumnParser> parsers,
                                  boolean isKey,
                                  int keySize,
                                  boolean isCheckData,
                                  Set<Integer> partitionIndexes) {
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                int idx = (int) tableSchema.getFieldIndex(columnName).get();
                Object columnObject = row.get(idx, tableSchema.apply(columnName).dataType());
                int parserIdx = i + (isKey ? 0 : keySize);
                if (isCheckData &&
                        !validateData(columnObject, baseIndex.getColumn(columnName), parsers.get(parserIdx), row)) {
                    return false;
                }
                columnObject = convertToJavaType(columnObject, tableSchema.apply(columnName).dataType());
                allColumns.add(columnObject);

                // partition key is also key
                if (isKey /*|| partitionIndexes.contains(idx)*/) {
                    keyColumns.add(columnObject);
                } else {
                    valColumns.add(columnObject);
                }
            }
            return true;
        }

        public List<Object> getAllColumns() {
            return allColumns;
        }

        public List<Object> getKeyColumns() {
            return keyColumns;
        }

        public List<Object> getValColumns() {
            return valColumns;
        }
    }

    public List<StarRocksRangePartitioner.PartitionRangeKey> createPartitionRangeKeys(
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws SparkWriteSDKException {
        List<StarRocksRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        if (PartitionType.UNPARTITIONED.not(partitionInfo.getPartitionType())
                && PartitionType.RANGE.not(partitionInfo.getPartitionType())) {
            return partitionRangeKeys;
        }
        for (EtlJobConfig.EtlPartition partition : partitionInfo.getPartitions()) {
            StarRocksRangePartitioner.PartitionRangeKey partitionRangeKey =
                    new StarRocksRangePartitioner.PartitionRangeKey();

            if (!partition.isMinPartition()) {
                partitionRangeKey.isMinPartition = false;
                List<Object> startKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.getStartKeys().size(); i++) {
                    Object value = partition.getStartKeys().get(i);
                    startKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.startKeys = new DppColumns(startKeyColumns);
            } else {
                partitionRangeKey.isMinPartition = true;
            }

            if (!partition.isMaxPartition()) {
                partitionRangeKey.isMaxPartition = false;
                List<Object> endKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.getEndKeys().size(); i++) {
                    Object value = partition.getEndKeys().get(i);
                    endKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.endKeys = new DppColumns(endKeyColumns);
            } else {
                partitionRangeKey.isMaxPartition = true;
            }

            partitionRangeKeys.add(partitionRangeKey);
        }
        return partitionRangeKeys;
    }

    public List<StarRocksListPartitioner.PartitionListKey> createPartitionListKeys(
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws SparkWriteSDKException {
        List<StarRocksListPartitioner.PartitionListKey> partitionListKeys = new ArrayList<>();
        if (PartitionType.LIST.not(partitionInfo.getPartitionType())) {
            return partitionListKeys;
        }

        for (EtlJobConfig.EtlPartition partition : partitionInfo.getPartitions()) {
            StarRocksListPartitioner.PartitionListKey partitionListKey = new StarRocksListPartitioner.PartitionListKey();
            if (CollectionUtils.isNotEmpty(partition.getInKeys())) {
                for (List<Object> objectList : partition.getInKeys()) {
                    List<Object> inKeyColumns = new ArrayList<>();
                    for (int i = 0; i < objectList.size(); i++) {
                        Object value = objectList.get(i);
                        inKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                    }
                    partitionListKey.inKeys.add(new DppColumns(inKeyColumns));
                }
                partitionListKeys.add(partitionListKey);
            }
        }
        return partitionListKeys;
    }

    private Object convertToJavaType(Object value, DataType dataType) {
        if (value instanceof UTF8String && dataType == StringType) {
            return ((UTF8String) value).toString();
        }
        if (value instanceof Integer && dataType == DateType) {
            return DateTimeUtils.toJavaDate((Integer) value);
        }
        if (value instanceof Long && dataType == TimestampType) {
            return DateTimeUtils.toJavaTimestamp((long) value);
        }
        if (value instanceof ArrayData
                || value instanceof InternalRow
                || value instanceof MapData) {
            return CatalystTypeConverters.convertToScala(value, dataType);
        }
        return value;
    }

    // partition keys will be parsed into double from json
    // so need to convert it to partition columns' type
    private Object convertPartitionKey(Object srcValue, Class dstClass) throws SparkWriteSDKException {
        if (dstClass.equals(Float.class) || dstClass.equals(Double.class)) {
            return null;
        }

        if (dstClass.isInstance(srcValue)) {
            return srcValue;
        }

        // PartitionKey is initialized according to the value of Json deserialization,
        // because the data type is Double after deserialization,
        // so there will be a conditional judgment of "if (srcValue instanceof Double)"
        if (srcValue instanceof Double) {
            if (dstClass.equals(Short.class)) {
                return ((Double) srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return ((Double) srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
                return ((Double) srcValue).longValue();
            } else if (dstClass.equals(BigInteger.class)) {
                // TODO(wb) gson will cast origin value to double by default
                // when the partition column is largeint, this will cause error data
                // need fix it thoroughly
                return new BigInteger(srcValue.toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDate((int) srcValueDouble);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDatetime((long) srcValueDouble);
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else if (srcValue instanceof Integer) {
            if (dstClass.equals(Short.class)) {
                return ((Integer) srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return srcValue;
            } else if (dstClass.equals(Long.class)) {
                return ((Integer) srcValue).longValue();
            } else if (dstClass.equals(BigInteger.class)) {
                // TODO(wb) gson will cast origin value to double by default
                // when the partition column is largeint, this will cause error data
                // need fix it thoroughly
                return new BigInteger(((Integer) srcValue).toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                return convertToJavaDate((int) srcValue);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                return convertToJavaDatetime((long) srcValue);
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else {
            throw new SparkWriteSDKException("Unsupported partition key: " + srcValue);
        }
    }

    private java.sql.Timestamp convertToJavaDatetime(long src) {
        String dateTimeStr = Long.valueOf(src).toString();
        if (dateTimeStr.length() != 14) {
            throw new RuntimeException("invalid input date format for SparkDpp");
        }

        String year = dateTimeStr.substring(0, 4);
        String month = dateTimeStr.substring(4, 6);
        String day = dateTimeStr.substring(6, 8);
        String hour = dateTimeStr.substring(8, 10);
        String min = dateTimeStr.substring(10, 12);
        String sec = dateTimeStr.substring(12, 14);

        return java.sql.Timestamp.valueOf(String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, min, sec));
    }

    private java.sql.Date convertToJavaDate(int originDate) {
        int day = originDate & 0x1f;
        originDate >>= 5;
        int month = originDate & 0x0f;
        originDate >>= 4;
        int year = originDate;
        return java.sql.Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
    }

}

