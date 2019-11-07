# spark-ssv
spark-ssv is a Spark custom data source for reading and writing string separated values using the DataSouceV2 API

We can add to the functionality of Spark's in built reading and writing APIs by extending the following classes in the DataSourceV2 API:

- DataSourceReader (SSVDataSourceReader.scala)
- DataSourceWriter (SSVDataSourceWriter.scala)
-	DataWriter (SSVDataWriter.scala)
-	DataWriterFactory (SSVDataWriterFactory.scala)
-	InputPartition (SSVInputPartition.scala)

Extending these classes with our own logic gives us more flexibility with reading and writing options and allows us to operate on the data differently for custom use cases.

**TODO:**

- Add more test cases for thorough testing in HDFS
- Add test cases for validation
- Give more flexibility to modify data for custom use cases
