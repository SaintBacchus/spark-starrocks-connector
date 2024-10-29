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
import com.starrocks.connector.spark.rest.models.FieldType;
import com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;
import com.starrocks.connector.spark.util.EnvUtils;
import com.starrocks.connector.spark.util.SegmentLoadDqc;
import com.starrocks.connector.spark.util.StarRocksFormatLibConf;
import com.starrocks.connector.spark.util.StarRocksWriterUtils;
import com.starrocks.format.Chunk;
import com.starrocks.format.Column;
import com.starrocks.format.rest.RestClient;
import com.starrocks.format.rest.model.TabletCommitInfo;
import com.starrocks.proto.TabletSchema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.removePrefix;
import static com.starrocks.connector.spark.sql.conf.WriteStarRocksConfig.KEY_BUFFER_SIZE;

public class StarRocksBypassWriter extends StarRocksWriter {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksBypassWriter.class);
    private static int batchRowCount = 4096;

    private final WriteStarRocksConfig config;
    private final StarRocksSchema schema;
    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final String label;
    private final Long txnId;

    private TabletSchema.TabletSchemaPB pbSchema;
    private com.starrocks.format.StarRocksWriter srWriter;
    private Chunk chunk;
    private List<Column> columns = new ArrayList<>();
    private int readRowCountInBatch = 0;
    private long tabletId;
    private long backendId;
    private volatile boolean isFirstRecord = true;
    private String currentJobFilePath = null;
    private SegmentLoadDqc dqc = new SegmentLoadDqc();

    public StarRocksBypassWriter(WriteStarRocksConfig config,
                                 StarRocksSchema schema,
                                 int partitionId,
                                 long taskId,
                                 long epochId,
                                 String label,
                                 Long txnId) {
        this.config = config;
        this.schema = schema;
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.pbSchema = schema.getEtlTable().toPbTabletSchema();
        this.label = label;
        this.txnId = txnId;
        if (config.getOriginOptions().get(KEY_BUFFER_SIZE) != null) {
            batchRowCount = Integer.parseInt(config.getOriginOptions().get(KEY_BUFFER_SIZE));
        }
    }

    @Override
    public void open() {
        LOG.info("Open bypass writer for partition: {}, task: {}, epoch: {}, batchSize: {}, label: {}, txnId: {}, {}",
                partitionId, taskId, epochId, batchRowCount, label, txnId, EnvUtils.getGitInformation());
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        readRowCountInBatch++;
        if (isFirstRecord) {
            newStarRocksWriter(internalRow);
            chunk = srWriter.newChunk(batchRowCount);
            IntStream.range(0, schema.getEtlTable().indexes.get(0).getColumns().size())
                    .forEach(i -> {
                        columns.add(chunk.getColumn(i));
                        dqc.collectDqcColumn(i, schema);
                    });
        }
        dqc.dqcProcess(internalRow, schema);
        dqc.increase();
        internalRow2Chunk(internalRow);
        if (readRowCountInBatch % batchRowCount == 0) {
            srWriter.write(chunk);
            chunk.reset();
            readRowCountInBatch = 0;
        }
        LOG.debug("Do write, label: {}, txnId: {}, partitionId: {}, taskId: {}, epochId: {}, receive raw row: {}",
                label, txnId, partitionId, taskId, epochId, internalRow);
    }

    @Override
    public WriterCommitMessage commit() {
        LOG.info("Commit write, label: {}, txnId: {}, partitionId: {}, taskId: {}, epochId: {}",
                label, txnId, partitionId, taskId, epochId);
        if (srWriter == null) {
            LOG.warn("StarRocksWriter should not be null");
            return new StarRocksWriterCommitMessage(partitionId, taskId, epochId, label, txnId,
                    null, null, dqc, currentJobFilePath);
        } else {
            srWriter.write(chunk);
            srWriter.flush();
            srWriter.finish();
            chunk.release();
        }

        return new StarRocksWriterCommitMessage(
                partitionId, taskId, epochId, label, txnId,
                null, new TabletCommitInfo(tabletId, backendId), dqc, currentJobFilePath);
    }

    @Override
    public void abort() throws IOException {
        LOG.info("Abort write, label: {}, txnId: {}, partitionId: {}, taskId: {}, epochId: {}",
                label, txnId, partitionId, taskId, epochId);
        dqc.reset();
        // clear current job file，if error，throw exception
        StarRocksWriterUtils.clearS3File(currentJobFilePath, ConfigUtils.getConfiguration(config.getOriginOptions()));
    }

    @Override
    public void close() throws IOException {
        LOG.info("Close bypass writer, partitionId: {}, taskId: {}, epochId: {}, label: {}, txnId: {}",
                partitionId, taskId, epochId, label, txnId);
        if (null != srWriter) {
            srWriter.close();
            srWriter.release();
        }
    }

    private void newStarRocksWriter(InternalRow internalRow) throws IOException {
        int schemaSize = schema.getColumns().size();
        tabletId = internalRow.getLong(schemaSize);
        backendId = schema.getBackendId(tabletId);
        String rootPath;
        if (config.isShareNothingBulkLoadEnabled()) {
            dqc.setTableId(tabletId);
            rootPath = schema.getStorageTabletPath(config.getWorkSpacePath(), tabletId) + "/" + UUID.randomUUID();
        }  else {
            rootPath = schema.getStoragePath(tabletId);
        }
        Map<String, String> configMap = removePrefix(config.getOriginOptions());
        currentJobFilePath = rootPath;
        LOG.info("Current job path path: {}", currentJobFilePath);
        if (config.isShareNothingBulkLoadEnabled()) {
            configMap.put("starrocks.format.mode", "share_nothing");
            if (schema.getEtlTable().getFastSchemaChange().equalsIgnoreCase("false")) {
                try (RestClient restClient = RestClientFactory.create(config)) {
                    if (schema.getMetadataUrl(tabletId).isEmpty()) {
                        throw new IllegalStateException("segment load for non fast schema should have meta url.");
                    }
                    String metaContext = restClient.getTabletMeta(schema.getMetadataUrl(tabletId));
                    configMap.put("starrocks.format.metaContext", metaContext);
                    configMap.put("starrocks.format.fastSchemaChange", "false");
                } catch (TransactionOperateException | IllegalStateException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException("validate tablet " + tabletId + " error: ", e);
                }
            }
        }
        try {
            StarRocksFormatLibConf formatLibConf = new StarRocksFormatLibConf(config.getOriginOptions());
            java.nio.file.Path currentPath = Paths.get("").toAbsolutePath();
            formatLibConf.writeConf(currentPath + "/starrocks.conf");
            InetAddress localHostAddress = InetAddress.getLocalHost();
            LOG.info("Current executor ip is: " + localHostAddress.getHostAddress());
        } catch (Exception e) {
            LOG.error("Set format lib conf error: ", e);
        }
        srWriter = new com.starrocks.format.StarRocksWriter(tabletId, pbSchema, txnId, rootPath, configMap);
        srWriter.open();
        isFirstRecord = false;
    }

    private void internalRow2Chunk(InternalRow internalRow) {
        int dstTableColumnSize = internalRow.numFields() - 1;
        assert dstTableColumnSize == columns.size();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            if (internalRow.isNullAt(i)) {
                column.appendNull();
                continue;
            }

            StarRocksField field = schema.getColumns().get(i);
            switch (FieldType.of(field.getType())) {
                case BOOLEAN:
                    column.appendBool(internalRow.getBoolean(i));
                    break;
                case TINYINT:
                    column.appendByte((byte) internalRow.getInt(i));
                    break;
                case SMALLINT:
                    column.appendShort(internalRow.getShort(i));
                    break;
                case INT:
                    column.appendInt(internalRow.getInt(i));
                    break;
                case BIGINT:
                    column.appendLong(internalRow.getLong(i));
                    break;
                case LARGEINT:
                    column.appendLargeInt(new BigInteger(internalRow.getString(i), 10));
                    break;
                case FLOAT:
                    column.appendFloat(internalRow.getFloat(i));
                    break;
                case DOUBLE:
                    column.appendDouble(internalRow.getDouble(i));
                    break;
                case DECIMAL:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    column.appendDecimal(internalRow
                            .getDecimal(i, field.getPrecision(), field.getScale()).toJavaBigDecimal());
                    break;
                case CHAR:
                case VARCHAR:
                case JSON:
                    column.appendString(internalRow.getString(i));
                    break;
                case BINARY:
                case VARBINARY:
                    column.appendBinary(internalRow.getBinary(i));
                    break;
                case DATE:
                    column.appendDate(DateTimeUtils.toJavaDate(internalRow.getInt(i)));
                    break;
                case DATETIME:
                    column.appendTimestamp(DateTimeUtils.toJavaTimestamp(internalRow.getLong(i)));
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported column type: " + field.getType());
            }
        }
    }

}
