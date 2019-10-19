package com.ashwinswaroop.sparkssv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String

class SSVInputPartitionReader(value: Int, separator: String, path: String, hasHeader: Boolean = true) extends InputPartitionReader[InternalRow] {

  var iterator: Iterator[String] = _

  @transient
  override def next(): Boolean = {
    if (iterator == null) {
      val sparkContext = SparkSession.builder.getOrCreate().sparkContext
      val rdd = sparkContext.textFile(path)
      iterator = rdd.compute(rdd.partitions(value), org.apache.spark.TaskContext.get())
      if(hasHeader)
        iterator.next()
      return iterator.hasNext
    }
    return iterator.hasNext
  }

  override def get(): InternalRow = {
    InternalRow.fromSeq(UTF8String.fromString(iterator.next()).split(UTF8String.fromString(separator),0))
  }

  override def close(): Unit = {

  }

}
