package com.starrocks.connector.spark.util

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}

import java.util.Optional
import scala.collection.JavaConverters._
import scala.language.implicitConversions

object Preamble {

  /**
   * Convert java Optional to scala Option.
   */
  implicit class OptionConverter[T](optional: Optional[T]) {

    def asScala: Option[T] = {
      if (optional.isPresent) Some(optional.get()) else None
    }

  }

  /**
   * Convert an arrow batch container into an iterator of InternalRow.
   */
  implicit class VectorSchemaRootIterator(root: VectorSchemaRoot) {

    def iterator: Iterator[InternalRow] = {
      val columns = root.getFieldVectors.asScala.map { vector =>
        new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
      }.toArray

      val batch = new ColumnarBatch(columns)
      batch.setNumRows(root.getRowCount)
      batch.rowIterator().asScala
    }

  }

}
