package com.ashwinswaroop.sparkssv

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

object SSVDataSourceTests extends FunSuite {

  val sparkSession = SparkSession.builder
    .master("local[*]")
    .appName("spark-ssv")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    SSVDataSourceTests.execute()
  }

  /* HDFS testing configurations
   sparkSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:9000")
   sparkSession.sparkContext.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "NEVER")
   sparkSession.sparkContext.hadoopConfiguration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
   */


  //TODO:
  //Add more test cases for thorough testing in HDFS
  //Add test cases for validation
  //Organize strings and constants

  test("Test to perform a basic read call") {
    val ssvDf = sparkSession.read
      .format("com.ashwinswaroop.sparkssv").option("separator", ",").load("/Users/ashwinswaroop/files/input.ssv")
    val ssvDfCount = ssvDf.count()
    print("Count of ssv DF: "+ssvDfCount)
    assert(ssvDfCount > 0)
  }

  test("Test to read a string separated file") {
    val ssvDf = sparkSession.read
      .format("com.ashwinswaroop.sparkssv").option("separator", "|||").load("/Users/ashwinswaroop/files/output")
    val ssvDfCount = ssvDf.count()
    print("Count of ssv DF: "+ssvDfCount)
    assert(ssvDfCount > 0)
  }


  test("Test to perform a basic write call") {
    val ssvDf = sparkSession.read
      .format("com.ashwinswaroop.sparkssv").option("separator", ",").load("/Users/ashwinswaroop/files/input.ssv")
    ssvDf.write.format("com.ashwinswaroop.sparkssv").option("separator", "|||").save("/Users/ashwinswaroop/files/output")
    //Assertion will change for hdfs based on settings
    assert(new java.io.File("/Users/ashwinswaroop/files/output").exists)
  }

  test("Test to ensure that the output string separated file is readable as a data frame") {
    val ssvDf = sparkSession.read
      .format("com.ashwinswaroop.sparkssv").option("separator", ",").load("/Users/ashwinswaroop/files/input.ssv")
    ssvDf.write.format("com.ashwinswaroop.sparkssv").option("separator", "|||").save("/Users/ashwinswaroop/files/output")
    val ssvDfResult = sparkSession.read
      .format("com.ashwinswaroop.sparkssv").option("separator", "|||").load("/Users/ashwinswaroop/files/output")
    val ssvDfResultCount = ssvDfResult.count()
    print("Count of result DF: "+ssvDfResultCount)
    assert(ssvDfResultCount > 0)
  }

}
