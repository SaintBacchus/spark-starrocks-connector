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

package org.apache.spark.sql.execution.datasources.v2

import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig.EtlIndex
import com.starrocks.connector.spark.sql.preprocessor._
import com.starrocks.connector.spark.sql.write.StarRocksWrite
import org.apache.spark.TaskContext
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{BatchWrite, PhysicalWriteInfoImpl, WriterCommitMessage}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.LongAccumulator

import java.util
import java.util.List
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

case class StarRocksWriteExec(batchWrite: BatchWrite,
                              query: SparkPlan,
                              writeMetrics: Seq[CustomMetric]) extends V2TableWriteExec {

  override val customMetrics: Map[String, SQLMetric] = writeMetrics.map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  override protected def run(): Seq[InternalRow] = {
    val rdd: RDD[InternalRow] = {
      val tempRdd = query.execute()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[InternalRow], 1)
      } else {
        tempRdd
      }
    }
    val newRdd = processRDD(batchWrite, rdd)

    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(newRdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](newRdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write [${logMessage(batchWrite)}]" +
      s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics

    try {
      sparkContext.runJob(
        newRdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writerFactory, context, iter, useCommitCoordinator, writeMetrics),
        newRdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write is committing, ${logMessage(batchWrite)}")
      batchWrite.commit(messages)
      logInfo(s"Data source write is committed, ${logMessage(batchWrite)}")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write is aborting, ${logMessage(batchWrite)}", cause)
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write is failed to abort, ${logMessage(batchWrite)}", t)
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(s"Data source write support is aborted, ${logMessage(batchWrite)}")
        throw cause
    }

    Nil
  }

  private def logMessage(batchWrite: BatchWrite): String = {
    batchWrite match {
      case srWrite: StarRocksWrite =>
        s"queryId: ${srWrite.getLogicalInfo.queryId()}, tableId: ${srWrite.getSchema.getTableId}"
      case _ =>
        s"not StarRocksWrite instance: ${batchWrite.getClass.getCanonicalName}"
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): StarRocksWriteExec =
    copy(query = newChild)


  protected def processRDD(batchWrite: BatchWrite, rdd: RDD[InternalRow]): RDD[InternalRow] = {
    if (!batchWrite.isInstanceOf[StarRocksWrite]) {
      throw new IllegalStateException(
        s"expected ${classOf[StarRocksWrite].getCanonicalName}, but got ${batchWrite.getClass.getCanonicalName}")
    }

    val schema = batchWrite.asInstanceOf[StarRocksWrite].getSchema;

    val fileGroupPartitions = new util.ArrayList[EtlJobConfig.EtlPartition]()
    val fileGroupPartitionRangeKeys = new util.ArrayList[StarRocksRangePartitioner.PartitionRangeKey]()
    val fileGroupPartitionListKeys = new util.ArrayList[StarRocksListPartitioner.PartitionListKey]()
    val partitionInfo = schema.getEtlTable.getPartitionInfo
    val partitionSize: Int = partitionInfo.getPartitions.size
    val preProcessor = new StarRocksPreProcessor(sparkContext.hadoopConfiguration)
    val baseIndex = fillRollupIndexes(schema.getEtlTable.getIndexes)
    val partitionKeySchema = fillPartitionSchema(baseIndex, schema.getEtlTable.getPartitionInfo.getPartitionColumnRefs)
    val partitionRangeKeys = preProcessor.createPartitionRangeKeys(partitionInfo, partitionKeySchema.schema)
    val partitionListKeys = preProcessor.createPartitionListKeys(partitionInfo, partitionKeySchema.schema)
    val columnNames = fillColumnNames(baseIndex)
    val dstTableSchema = DppUtils.createDstTableSchema(baseIndex.getColumns, false, true)
    val rollupTreeParser = new MinimumCoverageRollupTreeBuilder
    val rootNode = rollupTreeParser.build(schema.getEtlTable)
    val bucketKeyMap = new util.HashMap[String, Integer] {}
    val partitionKeyMap = new util.HashMap[java.lang.Long, EtlJobConfig.EtlPartition] {}
    var reduceNum = 0

    for (partitionIdx <- 0 until partitionSize) {
      val partition: EtlJobConfig.EtlPartition = partitionInfo.getPartitions.get(partitionIdx)
      partitionKeyMap.put(partition.getPartitionId, partition)
      for (bucketIdx <- 0 until partition.getBucketNum) {
        bucketKeyMap.put(partition.getPartitionId + "_" + bucketIdx, reduceNum)
        reduceNum += 1
      }
      fileGroupPartitions.add(partition)
      if (!partitionRangeKeys.isEmpty) {
        fileGroupPartitionRangeKeys.add(partitionRangeKeys.get(partitionIdx))
      }
      if (!partitionListKeys.isEmpty) {
        fileGroupPartitionListKeys.add(partitionListKeys.get(partitionIdx))
      }

    }

    val tablePairRDD: JavaPairRDD[List[AnyRef], Array[AnyRef]] = preProcessor
      .fillTupleWithPartitionColumn(
        partitionInfo,
        rdd,
        partitionKeySchema.index,
        fileGroupPartitionRangeKeys,
        fileGroupPartitionListKeys,
        columnNames.keys,
        columnNames.values,
        dstTableSchema,
        baseIndex
      )

    preProcessor.processRollupTree(rootNode, tablePairRDD, baseIndex, bucketKeyMap, partitionKeyMap)
  }

  def fillPartitionSchema(baseIndex: EtlJobConfig.EtlIndex, partitionColumnRefs: List[String]): PartitionKeySchema = {
    val keySchema: List[Class[_]] = new util.ArrayList[Class[_]]
    val keyIndex: util.List[Integer] = new util.ArrayList[Integer]
    for (partitionColName <- partitionColumnRefs.asScala) {
      breakable {
        for (i <- 0 until baseIndex.getColumns.size) {
          val column: EtlJobConfig.EtlColumn = baseIndex.getColumns.get(i)
          if (column.getColumnName == partitionColName) {
            keyIndex.add(i)
            keySchema.add(DppUtils.getClassFromColumn(column.getColumnType))
            break
          }
        }
      }
    }
    PartitionKeySchema(keyIndex, keySchema)
  }

  def fillRollupIndexes(indexes: List[EtlIndex]): EtlJobConfig.EtlIndex = {
    // get the base index meta
    var baseIndex: EtlJobConfig.EtlIndex = null
    breakable {
      for (indexMeta: EtlJobConfig.EtlIndex <- indexes.asScala) {
        if (indexMeta.isBaseIndex) {
          baseIndex = indexMeta
          break
        }
      }
    }
    baseIndex
  }

  def fillColumnNames(baseIndex: EtlJobConfig.EtlIndex): ColumnName = {
    // get key column names and value column names separately
    val keyColumnNames: util.List[String] = new util.ArrayList[String]
    val valueColumnNames: util.List[String] = new util.ArrayList[String]
    for (etlColumn: EtlJobConfig.EtlColumn <- baseIndex.getColumns.asScala) {
      if (etlColumn.isKey) {
        keyColumnNames.add(etlColumn.getColumnName)
      }
      else {
        valueColumnNames.add(etlColumn.getColumnName)
      }
    }
    ColumnName(keyColumnNames, valueColumnNames)
  }

  case class PartitionKeySchema(index: util.List[Integer], schema: util.List[Class[_]])

  case class ColumnName(keys: util.List[String], values: util.List[String])

}