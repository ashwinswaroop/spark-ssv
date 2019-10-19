package com.ashwinswaroop.sparkssv

import org.apache.spark.sql.SparkSession

object SSVDataSourceTests {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("spark-ssv")
      .getOrCreate()

    /* HDFS testing configurations
    sparkSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000")
    sparkSession.sparkContext.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "NEVER")
    sparkSession.sparkContext.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    */

    val ssvDf = sparkSession.read
      .format("com.ashwinswaroop.sparkssv").option("separator", ",").load("/Users/ashwinswaroop/files/input.ssv")
    ssvDf.show()
    ssvDf.write.format("com.ashwinswaroop.sparkssv").option("separator", "|||").save("/Users/ashwinswaroop/files/output")
  }

}
