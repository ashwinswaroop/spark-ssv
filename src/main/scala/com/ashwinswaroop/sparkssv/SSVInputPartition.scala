package com.ashwinswaroop.sparkssv

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

class SSVInputPartition(value: Int, separator: String, path: String) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new SSVInputPartitionReader(value, separator, path)
  }

}