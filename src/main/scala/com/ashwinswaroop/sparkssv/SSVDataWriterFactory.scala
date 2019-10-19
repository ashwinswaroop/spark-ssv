package com.ashwinswaroop.sparkssv

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class SSVDataWriterFactory(schema: StructType, options: util.Map[String, String]) extends DataWriterFactory[InternalRow] {

  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new SSVDataWriter(schema, options, partitionId, taskId, epochId)
  }

}
