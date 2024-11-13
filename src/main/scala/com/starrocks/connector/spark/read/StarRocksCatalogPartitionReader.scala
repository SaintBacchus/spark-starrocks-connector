package com.starrocks.connector.spark.read

import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import org.apache.spark.sql.connector.read.InputPartition


// For v2.0 Reader based on catalog
class StarRocksCatalogPartitionReader(partition: InputPartition,
                                      settings: Settings,
                                      schema: StarRocksSchema)
  extends AbstractStarRocksPartitionReader[StarRocksInternalRow](partition, settings, schema) {

  override def decode(values: Array[Any]): StarRocksInternalRow = {
    new StarRocksInternalRow(values)
  }

}