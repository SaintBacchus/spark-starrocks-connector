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

import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.format.rest.model.TabletCommitInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.util.Objects;
import java.util.StringJoiner;

public class StarRocksWriterCommitMessage implements WriterCommitMessage {

    private final int partitionId;
    private final long taskId;
    private final long epochId;

    private final String label;
    private final Long txnId;

    private final StreamLoadSnapshot snapshot;

    private final TabletCommitInfo tabletCommitInfo;

    public StarRocksWriterCommitMessage(int partitionId, long taskId, long epochId) {
        this(partitionId, taskId, epochId, null, null);
    }

    public StarRocksWriterCommitMessage(int partitionId,
                                        long taskId,
                                        long epochId,
                                        String label,
                                        Long txnId) {
        this(partitionId, taskId, epochId, label, txnId, null, null);
    }

    public StarRocksWriterCommitMessage(int partitionId,
                                        long taskId,
                                        long epochId,
                                        String label,
                                        Long txnId,
                                        StreamLoadSnapshot snapshot,
                                        TabletCommitInfo tabletCommitInfo) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.label = label;
        this.txnId = txnId;

        this.snapshot = snapshot;
        this.tabletCommitInfo = tabletCommitInfo;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getTaskId() {
        return taskId;
    }

    public long getEpochId() {
        return epochId;
    }

    public String getLabel() {
        return label;
    }

    public Long getTxnId() {
        return txnId;
    }

    public StreamLoadSnapshot getSnapshot() {
        return snapshot;
    }

    public TabletCommitInfo getTabletCommitInfo() {
        return tabletCommitInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StarRocksWriterCommitMessage that = (StarRocksWriterCommitMessage) o;
        return getPartitionId() == that.getPartitionId()
                && getTaskId() == that.getTaskId()
                && getEpochId() == that.getEpochId()
                && Objects.equals(getLabel(), that.getLabel())
                && Objects.equals(getTxnId(), that.getTxnId())
                && Objects.equals(getSnapshot(), that.getSnapshot())
                && Objects.equals(getTabletCommitInfo(), that.getTabletCommitInfo());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getPartitionId(), getTaskId(), getEpochId(), getLabel(), getTxnId(), getSnapshot(), getTabletCommitInfo()
        );
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", "[", "]")
                .add("partitionId=" + partitionId)
                .add("taskId=" + taskId)
                .add("epochId=" + epochId)
                .add("label='" + label + "'")
                .add("txnId=" + txnId)
                .add("snapshot=" + snapshot)
                .add("tabletCommitInfo=" + tabletCommitInfo)
                .toString();
    }
}
