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
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.util.EnvUtils;
import com.starrocks.format.rest.RestClient;
import com.starrocks.format.rest.model.TabletCommitInfo;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.removePrefix;
import static com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.KEY_BUFFER_SIZE;

public class StarRocksBypassWriter extends StarRocksWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksBypassWriter.class);

    private static int writeBufferSize = 4096;

    private final WriteStarRocksConfig config;
    private final StarRocksSchema srSchema;
    private final Schema arrowSchema;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final String label;
    private final Long txnId;

    private com.starrocks.format.StarRocksWriter srWriter;
    private VectorSchemaRoot root;
    private ArrowWriter arrowWriter;
    private int rowCountInBatch = 0;
    private long tabletId;
    private long backendId;
    private volatile boolean initialized = false;

    public StarRocksBypassWriter(WriteStarRocksConfig config,
                                 StarRocksSchema srSchema,
                                 int partitionId,
                                 long taskId,
                                 long epochId,
                                 String label,
                                 Long txnId) {
        this.config = config;
        this.srSchema = srSchema;
        this.arrowSchema = srSchema.getEtlTable().toArrowSchema(config.getTimeZone());
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.label = label;
        this.txnId = txnId;
        if (config.getOriginOptions().get(KEY_BUFFER_SIZE) != null) {
            writeBufferSize = Integer.parseInt(config.getOriginOptions().get(KEY_BUFFER_SIZE));
        }
    }

    @Override
    public void open() {
        LOG.info("Open bypass writer for partition: {}, task: {}, epoch: {}, bufferSize: {}, label: {}, txnId: {}, {}",
                partitionId, taskId, epochId, writeBufferSize, label, txnId, EnvUtils.getGitInformation());
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        if (!initialized) {
            openWriter(internalRow);
            root = VectorSchemaRoot.create(arrowSchema, srWriter.getRootAllocator());
            arrowWriter = ArrowWriter.create(root);
        }

        // Write InternalRow into VectorSchemaRoot
        arrowWriter.write(internalRow);
        if (++rowCountInBatch >= writeBufferSize) {
            flush();
        }

        LOG.debug("Do write, label: {}, txnId: {}, partitionId: {}, taskId: {}, epochId: {}, receive raw row: {}",
                label, txnId, partitionId, taskId, epochId, internalRow);
    }

    private void flush() {
        arrowWriter.finish();
        srWriter.write(arrowWriter.root());
        arrowWriter.reset();
        rowCountInBatch = 0;
    }

    @Override
    public WriterCommitMessage commit() {
        LOG.info("Commit write, label: {}, txnId: {}, partitionId: {}, taskId: {}, epochId: {}",
                label, txnId, partitionId, taskId, epochId);
        if (srWriter == null) {
            return new StarRocksWriterCommitMessage(partitionId, taskId, epochId, label, txnId);
        } else {
            if (rowCountInBatch > 0) {
                flush();
            }
            srWriter.flush();
            srWriter.finish();
        }
        return new StarRocksWriterCommitMessage(
                partitionId, taskId, epochId, label, txnId, null, new TabletCommitInfo(tabletId, backendId)
        );
    }

    @Override
    public void abort() throws IOException {
        LOG.info("Abort write, label: {}, txnId: {}, partitionId: {}, taskId: {}, epochId: {}",
                label, txnId, partitionId, taskId, epochId);
    }

    @Override
    public void close() throws IOException {
        LOG.info("Close bypass writer, partitionId: {}, taskId: {}, epochId: {}, label: {}, txnId: {}",
                partitionId, taskId, epochId, label, txnId);
        if (null != srWriter) {
            root.close();
            srWriter.close();
            srWriter.release();
        }
    }


    private void openWriter(InternalRow internalRow) {
        int schemaSize = srSchema.getColumns().size();
        tabletId = internalRow.getLong(schemaSize);
        backendId = srSchema.getBackendId(tabletId);
        String rootPath;
        if (config.isShareNothingBulkLoadEnabled()) {
            rootPath = srSchema.getStorageTabletPath(config.getShareNothingBulkLoadPath(), tabletId);
        }  else {
            rootPath = srSchema.getStoragePath(tabletId);
        }
        Map<String, String> configMap = removePrefix(config.getOriginOptions());
        if (config.isShareNothingBulkLoadEnabled()) {
            configMap.put("starrocks.format.mode", "share_nothing");
            try (RestClient restClient = RestClientFactory.create(config)) {
                if (srSchema.getMetadataUrl(tabletId).isEmpty()) {
                    throw new IllegalStateException("segment load for non fast schema should have meta url.");
                }
                String metaContext = restClient.getTabletMeta(srSchema.getMetadataUrl(tabletId));
                configMap.put("starrocks.format.metaContext", metaContext);
            } catch (TransactionOperateException | IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("validate tablet " + tabletId + " error: ", e);
            }
        }
        srWriter = new com.starrocks.format.StarRocksWriter(
                tabletId, txnId, arrowSchema, rootPath, configMap
        );
        srWriter.open();
        initialized = true;
    }

}
