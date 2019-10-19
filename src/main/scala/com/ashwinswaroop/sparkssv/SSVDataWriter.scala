package com.ashwinswaroop.sparkssv

import java.util
import java.io.{BufferedWriter, FileWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SSVDataWriter(schema: StructType, options: util.Map[String, String], partitionId: Int, taskId: Long, epochId: Long) extends DataWriter[InternalRow] {

  //Maybe we can move the writing part to commit
  override def write(record: InternalRow): Unit = {
    var line = ""
    schema.fields.indices.foreach(value ⇒ {
      if(value == 0)
        line = line+record.toSeq(schema)(value)
      else
        line = line+options.get("separator")+record.toSeq(schema)(value)
    })
    line = line+"\n"

    val writer = new BufferedWriter(
      new FileWriter(options.get("path"), true))
    writer.write(line)
    writer.close()
    /* Better option for hdfs since append is not guaranteed to work
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val path = new Path(options.get("path") + "/" + partitionId + "_" +taskId + "_" + epochId)
    var output: FSDataOutputStream = null
    if(fs.exists(path))
      output = fs.append(path)
    else
      output = fs.create(path)
    val os = new io.BufferedOutputStream(output)
    os.write(line.getBytes("UTF-8"))
    os.close()
    output.close()
    */
  }

  override def commit(): WriterCommitMessage = {

    object WriteSucceeded extends WriterCommitMessage
    WriteSucceeded
  }

  override def abort(): Unit = {

  }

}

