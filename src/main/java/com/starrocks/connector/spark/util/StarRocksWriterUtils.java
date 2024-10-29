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

package com.starrocks.connector.spark.util;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class StarRocksWriterUtils {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksWriterUtils.class);
    public static void clearS3File(String path, Configuration conf) {
        if (path == null || conf == null) {
            return;
        }
        try {
            FileSystem fs = FileSystem.get(new URI(path), conf);
            LOG.info("Try to delete {}...", path);
            if (!fs.exists(new Path(path))) {
                LOG.info("{} is not exist, skip delete.", path);
                return;
            }
            fs.delete(new Path(path), true);
        } catch (Exception e) {
            LOG.info("Delete {} failed, error is: ", path, e);
            throw new RuntimeException(e);
        }
    }

    public static void writeToS3(String data, String s3Path, Configuration conf) {
        if (data == null || s3Path == null || conf == null) {
            return;
        }
        String localTempFilePathStr = "/tmp/dqc_result_" + System.currentTimeMillis() + ".json";
        try {
            Path localTempFilePath = new Path(localTempFilePathStr);
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(Paths.get(localTempFilePathStr)))) {
                out.write(data.getBytes());
            }
            FileSystem fs = FileSystem.get(new URI(s3Path), conf);
            fs.copyFromLocalFile(true, localTempFilePath, new Path(s3Path));
        } catch (Exception e) {
            LOG.info("Write to s3 {} failed, error is: ", s3Path, e);
            throw new RuntimeException(e);
        }
    }

    public static void moveS3File(String source, String destination, Configuration conf) {
        if (source == null || destination == null || conf == null) {
            return;
        }
        try {
            Path srcPath = new Path(source);
            Path dstPath = new Path(destination);
            FileSystem fs = FileSystem.get(new URI(source), conf);
            LOG.info("Move {} to {}...", source, destination);
            boolean success = fs.mkdirs(dstPath);
            if (!success) {
                throw new RuntimeException("mkdir failed, path: " + destination);
            }
            success = fs.rename(srcPath, dstPath);
            if (!success) {
                throw new RuntimeException("move file failed, src: " + source + " dst: " + destination);
            }
        } catch (Exception e) {
            LOG.info("Move {} to {} failed, error is: ", source, destination, e);
            throw new RuntimeException(e);
        }
    }

    public static int murmur3_32(String str) {
        try {
            HashCode hashCode = Hashing.murmur3_32(104729).hashBytes(str.getBytes("utf-8"));
            return hashCode.asInt();
        } catch (Exception e) {
            LOG.error("Str {} hash hash execption: ", str, e);
            throw new RuntimeException(e);
        }
    }
}
