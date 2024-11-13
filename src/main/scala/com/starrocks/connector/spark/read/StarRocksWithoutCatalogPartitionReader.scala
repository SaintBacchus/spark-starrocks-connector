package com.starrocks.connector.spark.read

import com.starrocks.connector.spark.cfg.Settings
import org.apache.spark.sql.connector.read.InputPartition


// For v1.0 RDD Reader
class StarRocksWithoutCatalogPartitionReader(partition: InputPartition,
                                             settings: Settings)
  extends AbstractStarRocksPartitionReader[StarRocksRow](partition, settings, null) {

  override def decode(values: Array[Any]): StarRocksRow = {
    new StarRocksRow(values)
  }

}
