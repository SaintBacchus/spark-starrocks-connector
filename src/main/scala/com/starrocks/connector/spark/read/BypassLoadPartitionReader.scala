package com.starrocks.connector.spark.read

import com.starrocks.connector.spark.cfg.ConfigurationOptions.{STARROCKS_FORMAT_QUERY_PLAN, removePrefix}
import com.starrocks.connector.spark.cfg.Settings
import com.starrocks.connector.spark.rest.BypassPartition
import com.starrocks.connector.spark.sql.schema.StarRocksSchema
import com.starrocks.connector.spark.util.Preamble.VectorSchemaRootIterator
import com.starrocks.connector.spark.util.{ConfigUtils, FieldUtils}
import com.starrocks.format.StarRocksReader
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class BypassLoadPartitionReader(partition: InputPartition,
                                settings: Settings,
                                schema: StarRocksSchema)
  extends PartitionReader[InternalRow] {

  private val log = LoggerFactory.getLogger(classOf[BypassLoadPartitionReader])

  private lazy val reader = {
    val valueReader = new ArrowValueReader(
      partition.asInstanceOf[BypassPartition], settings, schema
    )
    valueReader.init()
    valueReader
  }

  private var row: Option[InternalRow] = None

  override def next(): Boolean = {
    row = if (reader.hasNext) Some(reader.next()) else None
    row.nonEmpty
  }

  override def get(): InternalRow = row match {
    case Some(r) => r
    case None => throw new IllegalStateException("Null internal row.")
  }

  override def close(): Unit = reader.close()

  private class ArrowValueReader(partition: BypassPartition,
                                 settings: Settings,
                                 schema: StarRocksSchema)
    extends Iterator[InternalRow] with AutoCloseable {

    private var outputSchema: StarRocksSchema = _
    private var reader: StarRocksReader = _

    private var rowItr: Iterator[InternalRow] = _

    private val closeableRoots = mutable.Buffer.empty[VectorSchemaRoot]

    def init(): Unit = {
      val fields = Option(partition.getSelectClause) match {
        case Some(f) => f.split(""",\s*""").map(fn => FieldUtils.eraseQuote(fn)).toSeq
        case None => Seq.empty[String]
      }

      val nameToFieldMapping = schema.getColumns.asScala.map(t => t.getName -> t).toMap
      val outputFields = fields.map(fn => nameToFieldMapping.get(fn) match {
        case Some(field) => field
        case None => throw new IllegalStateException(s"filed '${fn}' not found in schema")
      }).toList.asJava

      outputSchema = new StarRocksSchema(outputFields)
      val requiredFieldNames = partition.getRequiredFieldNames
      val outputColumnNames = outputSchema.getColumns.asScala.map(_.getName).toList.asJava

      settings.removeProperty(STARROCKS_FORMAT_QUERY_PLAN)
      if (ConfigUtils.isFilterPushDownEnabled(settings)
        && StringUtils.isNotBlank(partition.getFilterClause)) {
        settings.setProperty(STARROCKS_FORMAT_QUERY_PLAN, partition.getQueryPlan)
      }


      val startMillis = System.currentTimeMillis()
      val etlTable = schema.getEtlTable
      val tz = ConfigUtils.resolveTimeZone(settings)
      reader = new StarRocksReader(
        partition.getTabletId,
        partition.getVersion,
        etlTable.toArrowSchema(requiredFieldNames.asScala.toList.asJava, tz),
        etlTable.toArrowSchema(outputColumnNames, tz),
        partition.getStoragePath,
        removePrefix(settings.getPropertyMap))
      reader.open()

      if (ConfigUtils.isVerbose(settings)) {
        val elapseMillis = System.currentTimeMillis() - startMillis
        Option(settings.getProperty(STARROCKS_FORMAT_QUERY_PLAN)) match {
          case Some(plan) => log.info(
            s"Open native reader for tablet ${partition.getTabletId} use ${elapseMillis}ms with query plan:\n$plan")
          case None => log.info(
            s"Open native reader for tablet ${partition.getTabletId} use ${elapseMillis}ms without query plan.")
        }

      }
    }

    override def hasNext: Boolean = {
      if (null != rowItr && rowItr.hasNext) {
        true
      } else {
        Option(reader.getNext) match {
          case Some(root) =>
            closeableRoots += root
            if (root.getRowCount > 0) {
              rowItr = root.iterator
              rowItr.hasNext
            } else {
              false
            }
          case None => false
        }
      }
    }

    override def next(): InternalRow = Option(rowItr) match {
      case Some(itr) => itr.next()
      case None => throw new IllegalStateException("Null row iterator.")
    }

    override def close(): Unit = {
      closeableRoots.filterNot(r => null == r).foreach(r => r.close())
      reader.close()
      reader.release()
    }

  }

}
