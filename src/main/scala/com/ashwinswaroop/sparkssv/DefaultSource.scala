package com.ashwinswaroop.sparkssv

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val path = options.get("path").get
    val separator = options.get("separator").get
    new SSVDataSourceReader(path, separator)
  }

  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new SSVDataSourceWriter(schema, options))
  }

}





