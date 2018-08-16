// Copyright © 2018 GoPro, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ==============================================================================


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
