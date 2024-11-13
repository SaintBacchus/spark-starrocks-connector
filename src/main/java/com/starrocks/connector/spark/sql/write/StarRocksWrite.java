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

package com.starrocks.connector.spark.sql.write;

import com.starrocks.connector.spark.exception.TransactionOperateException;
import com.starrocks.connector.spark.rest.RestClientFactory;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.preprocessor.EtlJobConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.sql.schema.TableIdentifier;
import com.starrocks.format.StarRocksWriter;
import com.starrocks.format.rest.RestClient;
import com.starrocks.format.rest.TransactionResult;
import com.starrocks.format.rest.TxnOperation;
import com.starrocks.format.rest.model.TabletCommitInfo;
import com.starrocks.format.rest.model.TabletFailInfo;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.removePrefix;

public class StarRocksWrite implements BatchWrite, StreamingWrite {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksWrite.class);

    private final TableIdentifier identifier;
    private final LogicalWriteInfo logicalInfo;
    private final WriteStarRocksConfig config;
    private final StarRocksSchema schema;

    public StarRocksWrite(TableIdentifier identifier,
                          LogicalWriteInfo logicalInfo,
                          WriteStarRocksConfig config,
                          StarRocksSchema schema) {
        this.identifier = identifier;
        this.logicalInfo = logicalInfo;
        this.config = config;
        this.schema = schema;
    }

    /* for batch write */

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        if (config.notBypassWrite()) {
            return new StarRocksWriterFactory(logicalInfo.schema(), schema, config);
        }

        // begin transaction
        final String label = this.randomLabel(identifier);
        try (RestClient restClient = RestClientFactory.create(config)) {
            TransactionResult txnResult = restClient.beginTransaction(
                    identifier.getCatalog(), identifier.getDatabase(), identifier.getTable(), label);
            if (txnResult.notOk()) {
                throw new TransactionOperateException(TxnOperation.TXN_BEGIN, label, txnResult.getMessage());
            }
            return new StarRocksWriterFactory(
                    logicalInfo.schema(), schema, config, txnResult.getLabel(), txnResult.getTxnId());
        } catch (TransactionOperateException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "begin transaction for table " + identifier.toFullName() + " error, label: " + label, e);
        }
    }

    @Override
    public boolean useCommitCoordinator() {
        return true;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (config.notBypassWrite()) {
            LOG.info("Commit batch query: {}, bypass: {}", logicalInfo.queryId(), config.isBypassWrite());
            return;
        }

        List<TabletCommitInfo> tabletCommitInfos = Arrays.stream(messages)
                .map(message -> (StarRocksWriterCommitMessage) message)
                .map(StarRocksWriterCommitMessage::getTabletCommitInfo)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        List<Long> writtenTabletIds = tabletCommitInfos.stream()
                .map(TabletCommitInfo::getTabletId)
                .collect(Collectors.toList());

        StarRocksWriterCommitMessage message = firstNonNullMessage(messages);
        final Long txnId = message.getTxnId();
        Schema arrowSchema = schema.getEtlTable().toArrowSchema(config.getTimeZone());
        List<EtlJobConfig.EtlPartition> partitions = schema.getEtlTable().getPartitionInfo().getPartitions();
        for (EtlJobConfig.EtlPartition partition : partitions) {
            String rootPath = partition.getStoragePath();
            List<Long> unwrittenTabletIds = partition.getTabletIds();
            List<Long> allBackendIds = partition.getBackendIds();
            Map<Long, Long> tablet2Backend = IntStream.range(0, unwrittenTabletIds.size())
                    .boxed()
                    .collect(Collectors.toMap(unwrittenTabletIds::get, allBackendIds::get));
            // only have unwritten tablets
            unwrittenTabletIds.removeAll(writtenTabletIds);

            // write txn log
            Map<String, String> configMap = removePrefix(config.getOriginOptions());
            unwrittenTabletIds.forEach(unwrittenTabletId -> {
                StarRocksWriter srWriter = new StarRocksWriter(
                        unwrittenTabletId, txnId, arrowSchema, rootPath, configMap);
                // will write txn log
                srWriter.open();
                srWriter.finish();
                srWriter.close();
                srWriter.release();

                // add commit info
                tabletCommitInfos.add(
                        new TabletCommitInfo(unwrittenTabletId, tablet2Backend.get(unwrittenTabletId)));
            });
        }

        final String label = message.getLabel();
        try (RestClient restClient = RestClientFactory.create(config)) {
            TransactionResult prepareTxnResult = restClient.prepareTransaction(
                    identifier.getCatalog(),
                    identifier.getDatabase(),
                    label,
                    tabletCommitInfos,
                    null);
            if (prepareTxnResult.notOk()) {
                throw new TransactionOperateException(
                        TxnOperation.TXN_PREPARE, label, prepareTxnResult.getMessage());
            }

            TransactionResult commitTxnResult = restClient.commitTransaction(
                    identifier.getCatalog(), identifier.getDatabase(), label);
            if (commitTxnResult.notOk()) {
                throw new TransactionOperateException(
                        TxnOperation.TXN_COMMIT, label, commitTxnResult.getMessage());
            }
            LOG.info("Commit batch query: {}, bypass: {}, label: {}",
                    logicalInfo.queryId(), config.isBypassWrite(), label);
        } catch (TransactionOperateException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("prepare or commit transaction error, label: " + label, e);
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        if (config.notBypassWrite()) {
            LOG.info("Abort batch query: {}, bypass: {}", logicalInfo.queryId(), config.isBypassWrite());
            return;
        }

        List<TabletFailInfo> tabletFailInfos = Arrays.stream(messages)
                .map(writerCommitMessage -> (StarRocksWriterCommitMessage) writerCommitMessage)
                .map(StarRocksWriterCommitMessage::getTabletCommitInfo)
                .filter(Objects::nonNull)
                .map(commitTablet -> new TabletFailInfo(commitTablet.getTabletId(), commitTablet.getBackendId()))
                .collect(Collectors.toList());

        final String label = firstNonNullMessage(messages).getLabel();
        try (RestClient restClient = RestClientFactory.create(config)) {
            TransactionResult rollbackTxnResult = restClient.rollbackTransaction(
                    identifier.getCatalog(), identifier.getDatabase(), label, tabletFailInfos);
            if (rollbackTxnResult.notOk()) {
                throw new TransactionOperateException(
                        TxnOperation.TXN_ROLLBACK, label, rollbackTxnResult.getMessage());
            }
            LOG.info("Abort batch query: {}, bypass: {}, label: {}",
                    logicalInfo.queryId(), config.isBypassWrite(), label);
        } catch (TransactionOperateException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("rollback transaction error, label: " + label, e);
        }
    }

    private String randomLabel(TableIdentifier identifier) {
        return String.format("%s_%s_%s_%s",
                        config.getLabelPrefix(),
                        identifier.getDatabase(),
                        identifier.getTable(),
                        RandomStringUtils.randomAlphabetic(8))
                .toLowerCase();
    }

    private static StarRocksWriterCommitMessage firstNonNullMessage(WriterCommitMessage[] messages) {
        if (ArrayUtils.isEmpty(messages)) {
            throw new IllegalStateException("Empty commit messages");
        }

        return Arrays.stream(messages)
                .filter(Objects::nonNull)
                .map(message -> (StarRocksWriterCommitMessage) message)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Can't find any non-null commit message: " + Arrays.toString(messages)));
    }

    /* for streaming write */

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return new StarRocksWriterFactory(logicalInfo.schema(), schema, config);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        LOG.info("streaming query `{}` commit", logicalInfo.queryId());
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        LOG.info("streaming query `{}` abort", logicalInfo.queryId());
    }

    public TableIdentifier getIdentifier() {
        return identifier;
    }

    public LogicalWriteInfo getLogicalInfo() {
        return logicalInfo;
    }

    public WriteStarRocksConfig getConfig() {
        return config;
    }

    public StarRocksSchema getSchema() {
        return schema;
    }
}
