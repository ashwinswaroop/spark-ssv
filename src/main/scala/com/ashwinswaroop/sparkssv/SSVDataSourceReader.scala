package com.ashwinswaroop.sparkssv

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class SSVDataSourceReader(path: String, separator: String) extends DataSourceReader {

  override def readSchema(): StructType = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val firstLine = sparkContext.textFile(path).first()
    val columnNames = firstLine.split(separator)
    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
    StructType(structFields)
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val rdd = sparkContext.textFile(path)
    val inputPartitionList = new util.ArrayList[InputPartition[InternalRow]]
    (0 until rdd.getNumPartitions - 1).foreach(value ⇒
      inputPartitionList.add(new SSVInputPartition(value, separator, path)))
    inputPartitionList
  }

}
