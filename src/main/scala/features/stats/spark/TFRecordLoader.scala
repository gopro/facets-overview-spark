package features.stats.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Load TFRecords into Spark DataFrame via
  * Tensorflow/ecosystem/spark-tensorflow-connector
  */

object TFRecordLoader {

   def loadTFRecords(spark:SparkSession,
                     path:String,
                     recordType:TFRecordType = TFRecordType.Example,
                     schema:Option[StructType] = None): DataFrame = {

     val reader = spark.read.format("tfrecords").option("recordType", recordType.name())
     val dfReader = schema.map(reader.schema).getOrElse(reader)
     dfReader.load(path)
   }



}
