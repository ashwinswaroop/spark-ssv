package com.ashwinswaroop.sparkssv

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SSVDataSourceWriter(schema: StructType, options: DataSourceOptions) extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new SSVDataWriterFactory(schema, options.asMap())
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }

}
