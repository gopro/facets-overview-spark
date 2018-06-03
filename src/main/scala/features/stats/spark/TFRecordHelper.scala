package features.stats.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  * Load TFRecords into Spark DataFrame via
  * Tensorflow/ecosystem/spark-tensorflow-connector
  */

object TFRecordHelper {

   def loadTFRecords(spark:SparkSession,
                     path:String,
                     recordType:TFRecordType = TFRecordType.Example,
                     schema:Option[StructType] = None): DataFrame = {

     val reader = spark.read.format("tfrecords").option("recordType", recordType.name())
     val dfReader = schema.map(reader.schema).getOrElse(reader)
     dfReader.load(path)
   }


  def writerTFRecords(df: DataFrame,
                    path:String,
                    recordType:TFRecordType = TFRecordType.Example,
                    saveMode: Option[SaveMode]  = None): Unit = {

    val writer = saveMode.map(df.write.mode).getOrElse(df.write)
    writer.format("tfrecords").option("recordType",recordType.name()).save(path)

  }


}
