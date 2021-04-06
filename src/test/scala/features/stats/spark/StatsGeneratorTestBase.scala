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
//


package features.stats.spark

import java.io.File
import java.nio.file.{Files, Path, Paths}

import featureStatistics.feature_statistics.DatasetFeatureStatisticsList
import features.stats.ProtoUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


abstract class StatsGeneratorTestBase extends FunSuite with BeforeAndAfterAll {
  var rootTmpDir : Path = Files.createTempDirectory("spark_root").toAbsolutePath
  val appName    : String = "protoGenerator"
  val generator = new FeatureStatsGenerator(DatasetFeatureStatisticsList())
  val warehouseDir: Path = Files.createTempDirectory(rootTmpDir, "spark_warehouse")
  val spark : SparkSession = SparkSession.builder
                                        .appName(appName)
                                        .enableHiveSupport()
                                        .config("spark.driver.memory", "1.5g")
                                        .config("spark.sql.warehouse.dir", s"${warehouseDir.toString}/spark_warehouse" )
                                        .master("local[2]")
                                        .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
  }


  override def afterAll(): Unit = {
    import scala.reflect.io.Directory
    if (rootTmpDir != null) {
      val directory = new Directory(rootTmpDir.toFile)
      directory.deleteRecursively()
    }
  }



  def persistProto(proto: DatasetFeatureStatisticsList, base64Encode: Boolean, file: File ):Unit = {
    ProtoUtils.persistProto(proto, base64Encode, file)
  }


  def loadProto(base64Encode: Boolean, file: File ): DatasetFeatureStatisticsList = {
    ProtoUtils.loadProto(base64Encode, file)
  }

  def toJson(proto: DatasetFeatureStatisticsList) : String = {
    ProtoUtils.toJson(proto)
  }


  def loadCSVFile(filePath: String) : DataFrame = {
    spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "false") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(filePath)
  }


}
