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

package com.starrocks.connector.spark.sql

import com.starrocks.connector.spark.exception.StarRocksException
import com.starrocks.connector.spark.sql.schema.{StarRocksField, StarRocksSchema}
import com.starrocks.thrift.{TPrimitiveType, TScanColumnDesc}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters.seqAsJavaListConverter

class TestSchemaUtils {

  @Test
  def testGetCatalystType(): Unit = {
    assertEquals(DataTypes.NullType, SchemaUtils.getCatalystType("NULL_TYPE", 0, 0))
    assertEquals(DataTypes.BooleanType, SchemaUtils.getCatalystType("BOOLEAN", 0, 0))
    assertEquals(DataTypes.ByteType, SchemaUtils.getCatalystType("TINYINT", 0, 0))
    assertEquals(DataTypes.ShortType, SchemaUtils.getCatalystType("SMALLINT", 0, 0))
    assertEquals(DataTypes.IntegerType, SchemaUtils.getCatalystType("INT", 0, 0))
    assertEquals(DataTypes.LongType, SchemaUtils.getCatalystType("BIGINT", 0, 0))
    assertEquals(DataTypes.FloatType, SchemaUtils.getCatalystType("FLOAT", 0, 0))
    assertEquals(DataTypes.DoubleType, SchemaUtils.getCatalystType("DOUBLE", 0, 0))
    assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("DATE", 0, 0))
    assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("DATETIME", 0, 0))
    assertEquals(DataTypes.BinaryType, SchemaUtils.getCatalystType("BINARY", 0, 0))
    assertEquals(DecimalType(9, 3), SchemaUtils.getCatalystType("DECIMAL", 9, 3))
    assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("CHAR", 0, 0))
    assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("LARGEINT", 0, 0))
    assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("VARCHAR", 0, 0))
    assertEquals(DecimalType(10, 5), SchemaUtils.getCatalystType("DECIMALV2", 10, 5))
    assertEquals(DecimalType(12, 6), SchemaUtils.getCatalystType("DECIMAL64", 12, 6))
    assertEquals(DecimalType(30, 7), SchemaUtils.getCatalystType("DECIMAL128", 30, 7))
    assertEquals(DataTypes.DoubleType, SchemaUtils.getCatalystType("TIME", 0, 0))

    var exception = assertThrows(classOf[StarRocksException],
      () => {
        SchemaUtils.getCatalystType("HLL", 0, 0)
      })
    assertTrue(exception.getMessage().startsWith("Unsupported type"))

    exception = assertThrows(classOf[StarRocksException],
      () => {
        SchemaUtils.getCatalystType("UNRECOGNIZED", 0, 0)
      })
    assertTrue(exception.getMessage().startsWith("Unsupported type"))
  }

  /*@Test
  def testConvertToSchema(): Unit = {
    val k1 = new TScanColumnDesc
    k1.setName("k1")
    k1.setType(TPrimitiveType.BOOLEAN)

    val k2 = new TScanColumnDesc
    k2.setName("k2")
    k2.setType(TPrimitiveType.DOUBLE)

    val expected = new StarRocksSchema(List(
      new StarRocksField("k1", "BOOLEAN", 1, 0, 0, 0),
      new StarRocksField("k2", "DOUBLE", 2, 0, 0, 0)
    ).asJava)

    assertEquals(expected, SchemaUtils.convert(Seq(k1, k2)))
  }*/

}
