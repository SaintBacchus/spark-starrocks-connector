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
import com.starrocks.connector.spark.rest.models._
import com.starrocks.connector.spark.sql.schema.{StarRocksField, StarRocksSchema}
import com.starrocks.connector.spark.util.Preamble.OptionConverter
import com.starrocks.format.rest.model.Column
import com.starrocks.thrift.TScanColumnDesc
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.types._

import java.util
import scala.collection.JavaConverters._

private[spark] object SchemaUtils {

  /**
   * translate StarRocks Type to Spark Catalyst type
   *
   * @param starrocksType StarRocks type
   * @param precision     decimal precision
   * @param scale         decimal scale
   * @return Spark Catalyst type
   */
  def getCatalystType(starrocksType: String, precision: Int, scale: Int): DataType = {
    starrocksType match {
      case "NULL_TYPE" => DataTypes.NullType
      case "BOOLEAN" => DataTypes.BooleanType
      case "TINYINT" => DataTypes.ByteType
      case "SMALLINT" => DataTypes.ShortType
      case "INT" => DataTypes.IntegerType
      case "BIGINT" => DataTypes.LongType
      case "FLOAT" => DataTypes.FloatType
      case "DOUBLE" => DataTypes.DoubleType
      case "DATE" => DataTypes.StringType
      case "DATETIME" => DataTypes.StringType
      case "BINARY" => DataTypes.BinaryType
      case "DECIMAL" => DecimalType(precision, scale)
      case "CHAR" => DataTypes.StringType
      case "LARGEINT" => DataTypes.StringType
      case "VARCHAR" => DataTypes.StringType
      case "DECIMALV2" => DecimalType(precision, scale)
      case "DECIMAL32" => DecimalType(precision, scale)
      case "DECIMAL64" => DecimalType(precision, scale)
      case "DECIMAL128" => DecimalType(precision, scale)
      case "TIME" => DataTypes.DoubleType
      case "JSON" => DataTypes.StringType
      // ARRAY HLL BITMAP
      case _ =>
        throw new StarRocksException("Unsupported type " + starrocksType)
    }
  }

  def convert(scanColumnDescs: Seq[TScanColumnDesc]): StarRocksSchema = {
    val fields = scanColumnDescs.toStream
      .map(desc => new StarRocksField(
        desc.getName,
        Option(desc.getType) match {
          case Some(t) =>
            val fieldType = new Column.Type
            fieldType.setName(t.name())
            fieldType
          case None => null
        }, 0, true))
      .toList
    new StarRocksSchema(fields.asJava)
  }

  //TODO schema only use
  def convert(schema: Schema): StarRocksSchema = {
    val columns: util.List[StarRocksField] = new util.ArrayList[StarRocksField]
    val pks: util.List[StarRocksField] = new util.ArrayList[StarRocksField]
    val fields: util.List[Field] = schema.getProperties
    if (CollectionUtils.isNotEmpty(fields)) for (i <- 0 until fields.size) {
      val field: Field = fields.get(i)
      val srField: StarRocksField = new StarRocksField(
        field.getName,
        field.getType.asScala match {
          case Some(typeName) =>
            val fieldType = new Column.Type
            fieldType.setName(typeName)
            fieldType.setColumnSize(field.getColumnSize)
            fieldType.setPrecision(field.getPrecision)
            fieldType.setScale(field.getScale)
            fieldType
          case None => null
        },
        i + 1, // start at 1
        true)
      columns.add(srField)
      if (field.getIsKey) pks.add(srField)
    }
    new StarRocksSchema(columns, pks, schema.getEtlTable, schema.getTableId)
  }

}
